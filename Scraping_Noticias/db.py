"""
db.py — Interfaz con PostgreSQL (Supabase).

La tabla `normas_noticias` acumula todas las normas y noticias.
La deduplicación se hace por `link` (ON CONFLICT DO NOTHING).
`fecha_scraping` registra cuándo se insertó cada fila → permite saber qué es "nuevo hoy".

Variable de entorno requerida:
  SUPABASE_DB_URL  — cadena de conexión PostgreSQL de Supabase
                     Ejemplo: postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres
                     Se obtiene en: Supabase Dashboard → Settings → Database → Connection string
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Set

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Carga automática del .env (si existe). No hace nada en GitHub Actions
# donde las variables vienen como secrets del entorno.
load_dotenv(Path(__file__).parent / ".env")

log = logging.getLogger("db")

DATABASE_URL = os.getenv("SUPABASE_DB_URL", "")

# ─────────────────────────────────────────────
# DDL
# ─────────────────────────────────────────────

_CREATE_TABLE = """
CREATE SCHEMA IF NOT EXISTS normas_noticias;

CREATE TABLE IF NOT EXISTS normas_noticias.normas_noticias (
    id             BIGSERIAL    PRIMARY KEY,
    fuente         TEXT         NOT NULL,
    link           TEXT         NOT NULL UNIQUE,
    res            TEXT,
    titular        TEXT,
    fecha_pub      TEXT,
    sumilla        TEXT,
    contenido      TEXT,
    relevante      TEXT,
    impacto        INTEGER,
    resumen        JSONB,
    fecha_scraping DATE         NOT NULL DEFAULT CURRENT_DATE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_nn_fecha   ON normas_noticias.normas_noticias (fecha_scraping);
CREATE INDEX IF NOT EXISTS idx_nn_fuente  ON normas_noticias.normas_noticias (fuente);
CREATE INDEX IF NOT EXISTS idx_nn_created ON normas_noticias.normas_noticias (created_at);
"""

# ─────────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────────

def _get_conn():
    if not DATABASE_URL:
        raise EnvironmentError(
            "SUPABASE_DB_URL no está definida. "
            "Configura la variable de entorno con la cadena de conexión de Supabase.\n"
            "La encuentras en: Supabase Dashboard → Settings → Database → Connection string (URI)"
        )
    from urllib.parse import urlparse
    _p = urlparse(DATABASE_URL)
    log.info("DB connect → user=%s host=%s port=%s", _p.username, _p.hostname, _p.port)
    return psycopg2.connect(DATABASE_URL, connect_timeout=15,
                            options="-c search_path=normas_noticias")


# ─────────────────────────────────────────────
# INICIALIZACIÓN
# ─────────────────────────────────────────────

def ensure_tables() -> None:
    """Crea la tabla y los índices si no existen. Seguro de llamar en cada ejecución."""
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(_CREATE_TABLE)
        conn.commit()
    log.info("Tabla normas_noticias verificada/creada.")


# ─────────────────────────────────────────────
# HELPERS INTERNOS
# ─────────────────────────────────────────────

def _to_row(record: dict) -> Optional[dict]:
    """
    Convierte un dict del scraper al esquema de la BD.
    Retorna None si el registro no tiene link (no se puede deduplicar).
    """
    link = str(record.get("link") or "").strip()
    if not link or link in ("nan", "None"):
        return None

    resumen = record.get("resumen")
    if isinstance(resumen, (list, dict)):
        resumen = json.dumps(resumen, ensure_ascii=False)
    elif resumen is not None:
        resumen = str(resumen)

    impacto = record.get("impacto")
    try:
        impacto = int(impacto) if impacto is not None else None
    except (ValueError, TypeError):
        impacto = None

    relevante = record.get("relevante")
    relevante = str(relevante) if relevante is not None else None

    return {
        "fuente":    record.get("fuente", ""),
        "link":      link,
        "res":       record.get("res"),
        "titular":   record.get("titular"),
        "fecha_pub": record.get("fecha_pub"),
        "sumilla":   record.get("sumilla"),
        "contenido": record.get("contenido"),
        "relevante": relevante,
        "impacto":   impacto,
        "resumen":   resumen,
    }


# ─────────────────────────────────────────────
# ESCRITURA
# ─────────────────────────────────────────────

_INSERT_SQL = """
INSERT INTO normas_noticias
    (fuente, link, res, titular, fecha_pub, sumilla, contenido, relevante, impacto, resumen)
VALUES
    (%(fuente)s, %(link)s, %(res)s, %(titular)s, %(fecha_pub)s,
     %(sumilla)s, %(contenido)s, %(relevante)s, %(impacto)s, %(resumen)s)
ON CONFLICT (link) DO NOTHING
RETURNING link;
"""

def upsert_records(records: List[dict]) -> List[dict]:
    """
    Inserta los registros que aún no existen en la BD (deduplicación por `link`).

    Returns:
        Lista de registros efectivamente insertados (nuevos).
        Los ya existentes se ignoran silenciosamente.
    """
    if not records:
        return []

    rows = [_to_row(r) for r in records]
    rows = [r for r in rows if r]  # descartar los sin link

    if not rows:
        log.warning("Ningún registro tiene 'link' válido — no se insertó nada.")
        return []

    inserted_links: Set[str] = set()
    with _get_conn() as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(_INSERT_SQL, row)
                result = cur.fetchone()
                if result:
                    inserted_links.add(result[0])
        conn.commit()

    new_records = [r for r in records if str(r.get("link", "")).strip() in inserted_links]
    log.info(
        "upsert_records: %d insertados / %d ya existían (total recibidos: %d)",
        len(new_records), len(rows) - len(new_records), len(rows),
    )
    return new_records


def update_summary(link: str, resumen: list, relevante: str) -> None:
    """Actualiza resumen y relevante de un registro ya insertado (para Energiminas + OpenAI)."""
    sql = """
    UPDATE normas_noticias
    SET resumen   = %s,
        relevante = %s
    WHERE link = %s;
    """
    resumen_json = json.dumps(resumen, ensure_ascii=False) if resumen else None
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (resumen_json, relevante, link))
        conn.commit()


# ─────────────────────────────────────────────
# LECTURA
# ─────────────────────────────────────────────

def get_new_today(fuente: Optional[str] = None) -> List[dict]:
    """
    Retorna los registros cuya `fecha_scraping` es hoy.
    Si se especifica `fuente`, filtra por ella.
    """
    sql = """
    SELECT fuente, link, res, titular, fecha_pub, sumilla,
           contenido, relevante, impacto, resumen, created_at
    FROM   normas_noticias
    WHERE  fecha_scraping = CURRENT_DATE
    """
    params: list = []
    if fuente:
        sql += " AND fuente = %s"
        params.append(fuente)
    sql += " ORDER BY created_at ASC;"

    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    result = []
    for row in rows:
        r = dict(row)
        # Deserializar resumen JSONB → list
        if r.get("resumen") and isinstance(r["resumen"], str):
            try:
                r["resumen"] = json.loads(r["resumen"])
            except Exception:
                pass
        # psycopg2 ya deserializa JSONB a dict/list, normalizar a list
        if isinstance(r.get("resumen"), dict):
            r["resumen"] = list(r["resumen"].values())
        result.append(r)

    return result


def links_in_db(links: List[str]) -> Set[str]:
    """Retorna el subconjunto de links que ya existen en la BD."""
    if not links:
        return set()
    sql = "SELECT link FROM normas_noticias WHERE link = ANY(%s);"
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (links,))
            rows = cur.fetchall()
    return {row[0] for row in rows}
