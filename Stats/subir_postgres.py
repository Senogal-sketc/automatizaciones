"""
subir_postgres.py — Carga datos IEOD a PostgreSQL.

Puede ejecutarse directamente leyendo los JSON ya generados:
    python subir_postgres.py --fecha 2026-04-09
    python subir_postgres.py --fecha-inicio 2026-04-01 --fecha-fin 2026-04-09
"""

import argparse
import json
import logging
import sys
from datetime import date, timedelta

import psycopg2
from psycopg2.extras import execute_values

import config  # credenciales PostgreSQL centralizadas
from extraccion_ieod import ruta_json

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper de conexión y carga bulk
# ---------------------------------------------------------------------------

def _conn():
    return psycopg2.connect(
        host=config.PG_HOST, port=config.PG_PORT,
        dbname=config.PG_DB, user=config.PG_USER, password=config.PG_PASSWORD,
    )


def _bulk_insert(create_sql: str, insert_sql: str, filas: list, tabla: str) -> int:
    """Crea tabla si no existe, inserta filas en bulk. Retorna filas insertadas."""
    if not filas:
        log.warning("Sin filas para insertar en %s.", tabla)
        return 0
    conn = _conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(create_sql)
                execute_values(cur, insert_sql, filas, page_size=500)
                insertadas = cur.rowcount
        log.info("Filas insertadas en %s: %d", tabla, insertadas)
        return insertadas
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# despacho_ejecutado
# ---------------------------------------------------------------------------

_CREATE_DESPACHO = """
CREATE TABLE IF NOT EXISTS despacho_ejecutado (
    fecha       DATE             NOT NULL,
    hora        TIME             NOT NULL,
    central     VARCHAR(200)     NOT NULL,
    produccion  DOUBLE PRECISION,
    PRIMARY KEY (fecha, hora, central)
);
"""
_INSERT_DESPACHO = """
INSERT INTO despacho_ejecutado (fecha, hora, central, produccion)
VALUES %s ON CONFLICT (fecha, hora, central) DO NOTHING;
"""


def subir_despacho(registros: list[dict]) -> int:
    filas = [(r["fecha"], r["hora"], r["central"], r["produccion"]) for r in registros]
    return _bulk_insert(_CREATE_DESPACHO, _INSERT_DESPACHO, filas, "despacho_ejecutado")


# ---------------------------------------------------------------------------
# eventos
# ---------------------------------------------------------------------------

_CREATE_EVENTOS = """
CREATE TABLE IF NOT EXISTS eventos (
    fecha            DATE         NOT NULL,
    empresa          VARCHAR(200) NOT NULL,
    equipo           VARCHAR(200) NOT NULL,
    inicio           TIMESTAMP    NOT NULL,
    tipo_evento      VARCHAR(50),
    ubicacion        VARCHAR(200),
    tipo_equipo      VARCHAR(100),
    final            TIMESTAMP,
    descripcion      TEXT,
    mw_indisp        DOUBLE PRECISION,
    interrupcion     VARCHAR(5),
    tension_falla_kv DOUBLE PRECISION,
    PRIMARY KEY (fecha, empresa, equipo, inicio)
);
"""
_INSERT_EVENTOS = """
INSERT INTO eventos
    (fecha, empresa, equipo, inicio, tipo_evento, ubicacion, tipo_equipo,
     final, descripcion, mw_indisp, interrupcion, tension_falla_kv)
VALUES %s ON CONFLICT (fecha, empresa, equipo, inicio) DO NOTHING;
"""


def subir_eventos(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["empresa"], r["equipo"], r["inicio"], r["tipo_evento"],
         r["ubicacion"], r["tipo_equipo"], r["final"], r["descripcion"],
         r["mw_indisp"], r["interrupcion"], r["tension_falla_kv"])
        for r in registros if r.get("inicio")
    ]
    return _bulk_insert(_CREATE_EVENTOS, _INSERT_EVENTOS, filas, "eventos")


# ---------------------------------------------------------------------------
# restric_ope
# ---------------------------------------------------------------------------

_CREATE_RESTRIC = """
CREATE TABLE IF NOT EXISTS restric_ope (
    fecha       DATE         NOT NULL,
    empresa     VARCHAR(200) NOT NULL,
    equipo      VARCHAR(200) NOT NULL,
    hora_inicio VARCHAR(10)  NOT NULL,
    hora_fin    VARCHAR(10),
    ubicacion   VARCHAR(200),
    tipo_equipo VARCHAR(50),
    descripcion TEXT,
    PRIMARY KEY (fecha, empresa, equipo, hora_inicio)
);
"""
_INSERT_RESTRIC = """
INSERT INTO restric_ope
    (fecha, empresa, equipo, hora_inicio, hora_fin, ubicacion, tipo_equipo, descripcion)
VALUES %s ON CONFLICT (fecha, empresa, equipo, hora_inicio) DO NOTHING;
"""


def subir_restric_ope(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["empresa"], r["equipo"], r["hora_inicio"],
         r["hora_fin"], r["ubicacion"], r["tipo_equipo"], r["descripcion"])
        for r in registros if r.get("empresa") and r.get("equipo") and r.get("hora_inicio")
    ]
    return _bulk_insert(_CREATE_RESTRIC, _INSERT_RESTRIC, filas, "restric_ope")


# ---------------------------------------------------------------------------
# mantenimiento_ejecutados
# ---------------------------------------------------------------------------

_CREATE_MANT = """
CREATE TABLE IF NOT EXISTS mantenimiento_ejecutados (
    fecha           DATE         NOT NULL,
    empresa         VARCHAR(200) NOT NULL,
    equipo          VARCHAR(200) NOT NULL,
    inicio          TIMESTAMP    NOT NULL,
    ubicacion       VARCHAR(200),
    final           TIMESTAMP,
    descripcion     TEXT,
    mw_indisp       DOUBLE PRECISION,
    programado      VARCHAR(5),
    disponibilidad  VARCHAR(10),
    interrupcion    VARCHAR(5),
    tipo            VARCHAR(20),
    cod_eq          INTEGER,
    tipo_eq_osinerg VARCHAR(5),
    PRIMARY KEY (fecha, empresa, equipo, inicio)
);
"""
_INSERT_MANT = """
INSERT INTO mantenimiento_ejecutados
    (fecha, empresa, equipo, inicio, ubicacion, final, descripcion,
     mw_indisp, programado, disponibilidad, interrupcion, tipo, cod_eq, tipo_eq_osinerg)
VALUES %s ON CONFLICT (fecha, empresa, equipo, inicio) DO NOTHING;
"""


def subir_mantenimiento(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["empresa"], r["equipo"], r["inicio"], r["ubicacion"],
         r["final"], r["descripcion"], r["mw_indisp"], r["programado"],
         r["disponibilidad"], r["interrupcion"], r["tipo"], r["cod_eq"], r["tipo_eq_osinerg"])
        for r in registros if r.get("inicio")
    ]
    return _bulk_insert(_CREATE_MANT, _INSERT_MANT, filas, "mantenimiento_ejecutados")


# ---------------------------------------------------------------------------
# demanda_areas
# ---------------------------------------------------------------------------

_CREATE_DEMANDA = """
CREATE TABLE IF NOT EXISTS demanda_areas (
    fecha      DATE             NOT NULL,
    hora       TIME             NOT NULL,
    area       VARCHAR(100)     NOT NULL,
    demanda_mw DOUBLE PRECISION,
    PRIMARY KEY (fecha, hora, area)
);
"""
_INSERT_DEMANDA = """
INSERT INTO demanda_areas (fecha, hora, area, demanda_mw)
VALUES %s ON CONFLICT (fecha, hora, area) DO NOTHING;
"""


def subir_demanda_areas(registros: list[dict]) -> int:
    filas = [(r["fecha"], r["hora"], r["area"], r["demanda_mw"]) for r in registros]
    return _bulk_insert(_CREATE_DEMANDA, _INSERT_DEMANDA, filas, "demanda_areas")


# ---------------------------------------------------------------------------
# princip_caudales
# ---------------------------------------------------------------------------

_CREATE_CAUDALES = """
CREATE TABLE IF NOT EXISTS princip_caudales (
    fecha       DATE             NOT NULL,
    hora        TIME             NOT NULL,
    empresa     VARCHAR(200)     NOT NULL,
    equipo      VARCHAR(200)     NOT NULL,
    tipo_caudal VARCHAR(100)     NOT NULL,
    cuenca      VARCHAR(200),
    instalacion VARCHAR(200),
    caudal_m3s  DOUBLE PRECISION,
    PRIMARY KEY (fecha, hora, empresa, equipo, tipo_caudal)
);
"""
_INSERT_CAUDALES = """
INSERT INTO princip_caudales
    (fecha, hora, empresa, equipo, tipo_caudal, cuenca, instalacion, caudal_m3s)
VALUES %s ON CONFLICT (fecha, hora, empresa, equipo, tipo_caudal) DO NOTHING;
"""


def subir_princip_caudales(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["hora"], r["empresa"], r["equipo"], r["tipo_caudal"],
         r["cuenca"], r["instalacion"], r["caudal_m3s"])
        for r in registros
    ]
    return _bulk_insert(_CREATE_CAUDALES, _INSERT_CAUDALES, filas, "princip_caudales")


# ---------------------------------------------------------------------------
# princip_volumenes
# ---------------------------------------------------------------------------

_CREATE_VOLUMENES = """
CREATE TABLE IF NOT EXISTS princip_volumenes (
    fecha         DATE             NOT NULL,
    hora          TIME             NOT NULL,
    empresa       VARCHAR(200)     NOT NULL,
    equipo        VARCHAR(200)     NOT NULL,
    tipo_medicion VARCHAR(100)     NOT NULL,
    cuenca        VARCHAR(200),
    instalacion   VARCHAR(200),
    unidad        VARCHAR(20),
    valor         DOUBLE PRECISION,
    PRIMARY KEY (fecha, hora, empresa, equipo, tipo_medicion)
);
"""
_INSERT_VOLUMENES = """
INSERT INTO princip_volumenes
    (fecha, hora, empresa, equipo, tipo_medicion, cuenca, instalacion, unidad, valor)
VALUES %s ON CONFLICT (fecha, hora, empresa, equipo, tipo_medicion) DO NOTHING;
"""


def subir_princip_volumenes(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["hora"], r["empresa"], r["equipo"], r["tipo_medicion"],
         r["cuenca"], r["instalacion"], r["unidad"], r["valor"])
        for r in registros
    ]
    return _bulk_insert(_CREATE_VOLUMENES, _INSERT_VOLUMENES, filas, "princip_volumenes")


# ---------------------------------------------------------------------------
# consumo_comb
# ---------------------------------------------------------------------------

_CREATE_CONSUMO = """
CREATE TABLE IF NOT EXISTS consumo_comb (
    fecha            DATE             NOT NULL,
    empresa          VARCHAR(200)     NOT NULL,
    central          VARCHAR(200)     NOT NULL,
    medidor          VARCHAR(200)     NOT NULL,
    tipo_combustible VARCHAR(100)     NOT NULL,
    unidad           VARCHAR(20),
    consumo          DOUBLE PRECISION,
    PRIMARY KEY (fecha, empresa, central, medidor, tipo_combustible)
);
"""
_INSERT_CONSUMO = """
INSERT INTO consumo_comb (fecha, empresa, central, medidor, tipo_combustible, unidad, consumo)
VALUES %s ON CONFLICT (fecha, empresa, central, medidor, tipo_combustible) DO NOTHING;
"""


def subir_consumo_comb(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["empresa"], r["central"], r["medidor"],
         r["tipo_combustible"], r["unidad"], r["consumo"])
        for r in registros if r.get("empresa") and r.get("central") and r.get("medidor") and r.get("tipo_combustible")
    ]
    return _bulk_insert(_CREATE_CONSUMO, _INSERT_CONSUMO, filas, "consumo_comb")


# ---------------------------------------------------------------------------
# disponibilidad_gas
# ---------------------------------------------------------------------------

_CREATE_DISP_GAS = """
CREATE TABLE IF NOT EXISTS disponibilidad_gas (
    fecha         DATE             NOT NULL,
    empresa       VARCHAR(200)     NOT NULL,
    gaseoducto    VARCHAR(200)     NOT NULL,
    volumen_mm3   DOUBLE PRECISION,
    inicio        TIMESTAMP,
    final         TIMESTAMP,
    observaciones TEXT,
    PRIMARY KEY (fecha, empresa, gaseoducto)
);
"""
_INSERT_DISP_GAS = """
INSERT INTO disponibilidad_gas
    (fecha, empresa, gaseoducto, volumen_mm3, inicio, final, observaciones)
VALUES %s ON CONFLICT (fecha, empresa, gaseoducto) DO NOTHING;
"""


def subir_disponibilidad_gas(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["empresa"], r["gaseoducto"], r["volumen_mm3"],
         r["inicio"], r["final"], r["observaciones"])
        for r in registros if r.get("empresa") and r.get("gaseoducto")
    ]
    return _bulk_insert(_CREATE_DISP_GAS, _INSERT_DISP_GAS, filas, "disponibilidad_gas")


# ---------------------------------------------------------------------------
# interconexiones
# ---------------------------------------------------------------------------

_CREATE_INTERCON = """
CREATE TABLE IF NOT EXISTS interconexiones (
    fecha    DATE             NOT NULL,
    hora     TIME             NOT NULL,
    codigo   VARCHAR(20)      NOT NULL,
    linea    VARCHAR(200),
    grupo    VARCHAR(200),
    flujo_mw DOUBLE PRECISION,
    PRIMARY KEY (fecha, hora, codigo)
);
"""
_INSERT_INTERCON = """
INSERT INTO interconexiones (fecha, hora, codigo, linea, grupo, flujo_mw)
VALUES %s ON CONFLICT (fecha, hora, codigo) DO NOTHING;
"""


def subir_interconexiones(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["hora"], r["codigo"], r["linea"], r["grupo"], r["flujo_mw"])
        for r in registros if r.get("codigo")
    ]
    return _bulk_insert(_CREATE_INTERCON, _INSERT_INTERCON, filas, "interconexiones")


# ---------------------------------------------------------------------------
# costo_ope_ejec
# ---------------------------------------------------------------------------

_CREATE_COSTO = """
CREATE TABLE IF NOT EXISTS costo_ope_ejec (
    fecha             DATE             NOT NULL,
    costo_ejecutado   DOUBLE PRECISION,
    costo_programado  DOUBLE PRECISION,
    porcentaje        DOUBLE PRECISION,
    PRIMARY KEY (fecha)
);
"""
_INSERT_COSTO = """
INSERT INTO costo_ope_ejec (fecha, costo_ejecutado, costo_programado, porcentaje)
VALUES %s ON CONFLICT (fecha) DO NOTHING;
"""


def subir_costo_ope_ejec(registros: list[dict]) -> int:
    filas = [
        (r["fecha"], r["costo_ejecutado"], r["costo_programado"], r["porcentaje"])
        for r in registros if r.get("fecha")
    ]
    return _bulk_insert(_CREATE_COSTO, _INSERT_COSTO, filas, "costo_ope_ejec")


# ---------------------------------------------------------------------------
# subir_desde_json — lee el JSON de la fecha y sube todas las tablas
# ---------------------------------------------------------------------------

_SUBIR_FNS = {
    "despacho_ejecutado":       subir_despacho,
    "eventos":                  subir_eventos,
    "restric_ope":              subir_restric_ope,
    "mantenimiento_ejecutados": subir_mantenimiento,
    "demanda_areas":            subir_demanda_areas,
    "princip_caudales":         subir_princip_caudales,
    "princip_volumenes":        subir_princip_volumenes,
    "consumo_comb":             subir_consumo_comb,
    "disponibilidad_gas":       subir_disponibilidad_gas,
    "interconexiones":          subir_interconexiones,
    "costo_ope_ejec":           subir_costo_ope_ejec,
}


def subir_desde_json(fecha: date) -> None:
    """Lee el JSON de la fecha y sube cada clave a su tabla correspondiente."""
    ruta = ruta_json(fecha)
    if not ruta.exists():
        raise FileNotFoundError(f"JSON no encontrado: {ruta}")

    with open(ruta, "r", encoding="utf-8") as f:
        contenido = json.load(f)

    for clave, fn_subir in _SUBIR_FNS.items():
        registros = contenido.get(clave, [])
        if registros:
            fn_subir(registros)
        else:
            log.debug("[%s] JSON sin datos de %s.", fecha, clave)


# ---------------------------------------------------------------------------
# CLI directo
# ---------------------------------------------------------------------------

def _parse_args():
    p = argparse.ArgumentParser(description="Sube datos IEOD desde JSON a PostgreSQL.")
    p.add_argument("--fecha", default=None, help="Fecha YYYY-MM-DD (default: ayer)")
    p.add_argument("--fecha-inicio", default=None, help="Inicio de rango YYYY-MM-DD")
    p.add_argument("--fecha-fin", default=None, help="Fin de rango YYYY-MM-DD")
    return p.parse_args()


def _resolver_fechas(args) -> list[date]:
    if args.fecha_inicio and args.fecha_fin:
        inicio = date.fromisoformat(args.fecha_inicio)
        fin = date.fromisoformat(args.fecha_fin)
        return [inicio + timedelta(days=i) for i in range((fin - inicio).days + 1)]
    if args.fecha:
        return [date.fromisoformat(args.fecha)]
    return [date.today() - timedelta(days=1)]


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    _args = _parse_args()
    _fechas = _resolver_fechas(_args)
    _errores = 0
    for _fecha in _fechas:
        try:
            subir_desde_json(_fecha)
        except Exception as exc:
            log.error("[%s] Error: %s", _fecha, exc)
            _errores += 1
    sys.exit(1 if _errores else 0)
