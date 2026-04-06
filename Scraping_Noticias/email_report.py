"""
email_report.py — Genera y envía el reporte diario de normas/noticias por Gmail.

Uso:
  # Después de correr pipeline.py:
  python email_report.py

  # O importando directamente desde otro script:
  from email_report import build_and_send

Variables de entorno:
  BASE_DATA_DIR         — Directorio raíz donde pipeline.py guardó los JSONs
  GMAIL_CREDENTIALS     — Path a credentials.json  (default: credentials.json)
  GMAIL_TOKEN           — Path a token.json         (default: token.json)
  DESTINATARIOS         — Emails separados por coma (sobreescribe la lista interna)
"""

from __future__ import annotations

import base64
import json
import logging
import os
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

TZ = ZoneInfo("America/Lima")

# Destinatarios por defecto (sobreescribibles con la env var DESTINATARIOS)
_DEFAULT_DESTINATARIOS = [
    "egaray@grupomacro.pe",
    "evander.garay@gmail.com",
    # "ddiaz@grupomacro.pe",
    # "varce@grupomacro.pe",
    # "gtamayo@grupomacro.pe",
    # "vflores@grupomacro.pe",
]

GMAIL_SCOPES      = ["https://www.googleapis.com/auth/gmail.send"]
CREDENTIALS_FILE  = Path(os.getenv("GMAIL_CREDENTIALS", "credentials.json"))
TOKEN_FILE        = Path(os.getenv("GMAIL_TOKEN",        "token.json"))

# Directorio raíz de datos (mismo que usa pipeline.py)
_DEFAULT_BASE = (
    r"C:\Users\egaray\Macroconsult S.A\Soporte TI - REGCOM"
    r"\0_DATA\0.1_ELECTRICIDAD\0_BASES DE DATOS\Normas"
)
BASE_DATA_DIR = Path(os.getenv("BASE_DATA_DIR", _DEFAULT_BASE))

OUTDIRS = {
    "osinergmin": BASE_DATA_DIR / "Osinergmin_gob",
    "el_peruano": BASE_DATA_DIR / "El Peruano",
    "energiminas": BASE_DATA_DIR / "Energiminas",
    "minem":       BASE_DATA_DIR / "MINEM",
}

# Nombre del remitente que aparece al pie del correo
FIRMA = "Evander Garay"

log = logging.getLogger("email_report")

# ─────────────────────────────────────────────
# GMAIL AUTH
# ─────────────────────────────────────────────

def gmail_service():
    """
    Autentica con Gmail API.
    - En producción/CI: lee token.json (refresh automático sin UI).
    - Primera vez local: abre el navegador para autorizar.
    """
    creds: Optional[Credentials] = None

    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), GMAIL_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log.info("Refrescando token de Gmail…")
            creds.refresh(Request())
        else:
            if not CREDENTIALS_FILE.exists():
                raise FileNotFoundError(
                    f"No se encontró {CREDENTIALS_FILE}. "
                    "Descárgalo desde Google Cloud Console y colócalo junto a este script, "
                    "o define la variable de entorno GMAIL_CREDENTIALS."
                )
            log.info("Iniciando flujo OAuth (solo necesario la primera vez)…")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_FILE), GMAIL_SCOPES
            )
            creds = flow.run_local_server(port=0)

        # Guardar token para futuras ejecuciones
        TOKEN_FILE.write_text(creds.to_json(), encoding="utf-8")
        log.info("Token guardado en %s", TOKEN_FILE)

    return build("gmail", "v1", credentials=creds)


def _send_html(service, to: str, subject: str, html: str) -> None:
    msg = MIMEText(html, "html", "utf-8")
    msg["to"]      = to
    msg["subject"] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(userId="me", body={"raw": raw}).execute()
    log.info("Correo enviado → %s", to)


# ─────────────────────────────────────────────
# LECTURA DE DATOS
# ─────────────────────────────────────────────

def _load_today_json(source: str, date_str: str) -> pd.DataFrame:
    """
    Busca el JSON de hoy (YYYY_MM_DD_<Name>.json) en el directorio correspondiente.
    Retorna DataFrame vacío si no existe.
    """
    names = {
        "osinergmin": "Osinergmin_gob",
        "el_peruano": "EL_Peruano",
        "energiminas": "Energiminas",
        "minem": "MINEM",
    }
    outdir  = OUTDIRS[source]
    pattern = f"{date_str}_{names[source]}*.json"
    files   = sorted(outdir.glob(pattern))

    if not files:
        log.warning("No se encontró JSON de hoy para '%s' en %s (patrón: %s)",
                    source, outdir, pattern)
        return pd.DataFrame()

    records = []
    for fp in files:
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
            if isinstance(data, list):
                records.extend(data)
            elif isinstance(data, dict):
                records.append(data)
        except Exception as e:
            log.warning("Error leyendo %s: %s", fp.name, e)

    return pd.DataFrame(records) if records else pd.DataFrame()


def _to_bin(x) -> int:
    if pd.isna(x):
        return 0
    return 1 if str(x).strip().lower() in ("1", "true", "t", "yes", "y") else 0


# ─────────────────────────────────────────────
# CONSTRUCCIÓN HTML
# ─────────────────────────────────────────────

_CSS = """
<style>
  body { font-family: Arial, sans-serif; font-size: 13px; color: #222; }
  h2   { color: #1a3a5c; border-bottom: 2px solid #1a3a5c; padding-bottom: 4px; }
  h3   { color: #2e6da4; }
  table { border-collapse: collapse; width: 100%; margin-bottom: 16px; }
  th   { background: #1a3a5c; color: #fff; padding: 6px 8px; text-align: left; }
  td   { border: 1px solid #ccc; padding: 5px 8px; vertical-align: top; }
  tr:nth-child(even) td { background: #f5f8fc; }
  a    { color: #1a3a5c; }
  .empty { color: #888; font-style: italic; }
  ul   { margin: 0; padding-left: 18px; }
  li   { margin-bottom: 3px; }
</style>
"""


def _link(url: str, texto: str = "ver") -> str:
    u = str(url).strip()
    if not u:
        return ""
    return f'<a href="{u}" target="_blank" rel="noopener">{texto}</a>'


def _resumen_html(val) -> str:
    """Convierte lista de ideas o string a HTML."""
    if isinstance(val, list) and val:
        items = "".join(f"<li>{item}</li>" for item in val if item)
        return f"<ul>{items}</ul>"
    if isinstance(val, str) and val.strip():
        return val
    return ""


def _df_to_table(df: pd.DataFrame, cols: list, link_col: str = "link") -> str:
    if df.empty:
        return '<p class="empty">Sin registros en esta categoría.</p>'

    d = df.copy()
    for c in cols:
        if c not in d.columns:
            d[c] = ""

    headers = "".join(f"<th>{c}</th>" for c in cols)
    rows = []
    for _, row in d.iterrows():
        cells = []
        for c in cols:
            val = row.get(c, "")
            if c == link_col:
                cell = _link(val)
            elif c == "resumen":
                cell = _resumen_html(val)
            else:
                cell = str(val) if not pd.isna(val) else ""
            cells.append(f"<td>{cell}</td>")
        rows.append(f"<tr>{''.join(cells)}</tr>")

    return f"<table><tr>{headers}</tr>{''.join(rows)}</table>"


def _section_el_peruano(df: pd.DataFrame, fecha_hoy: str) -> str:
    if df.empty:
        return f"""
        <h2>El Peruano</h2>
        <p class="empty">No se encontraron datos para el {fecha_hoy}.</p>
        """

    df = df.copy()
    df["relevante_bin"] = df.get("relevante", pd.Series(0, index=df.index)).apply(_to_bin)
    df["impacto_bin"]   = df.get("impacto",   pd.Series(0, index=df.index)).apply(_to_bin)

    relev = df[df["relevante_bin"] == 1]
    imp1  = relev[relev["impacto_bin"] == 1]
    imp0  = relev[relev["impacto_bin"] == 0]
    cols  = ["res", "sumilla", "link"]

    if relev.empty:
        return f"""
        <h2>El Peruano</h2>
        <p class="empty">No se encontraron normas relevantes para el {fecha_hoy}.</p>
        """

    n_total   = len(df)
    n_relev   = len(relev)
    n_impacto = len(imp1)

    return f"""
    <h2>El Peruano</h2>
    <p>Total normas publicadas: <b>{n_total}</b> &nbsp;|&nbsp;
       Relevantes: <b>{n_relev}</b> &nbsp;|&nbsp;
       Con impacto eléctrico: <b>{n_impacto}</b></p>

    <h3>Normas con impacto eléctrico</h3>
    {_df_to_table(imp1, cols)}

    <h3>Otras normas relevantes</h3>
    {_df_to_table(imp0, cols)}
    """


def _section_energiminas(df: pd.DataFrame, fecha_hoy: str) -> str:
    if df.empty:
        return f"""
        <h2>Energiminas</h2>
        <p class="empty">No se encontraron datos para el {fecha_hoy}.</p>
        """

    df = df.copy()
    df["nueva_bin"]    = df.get("nueva_noticia", pd.Series(0, index=df.index)).apply(_to_bin)
    df["relevante_bin"] = df.get("relevante", pd.Series(1, index=df.index)).apply(_to_bin)

    nuevas = df[(df["nueva_bin"] == 1) & (df["relevante_bin"] == 1)]
    cols   = ["titular", "resumen", "link"]

    if nuevas.empty:
        return f"""
        <h2>Energiminas</h2>
        <p class="empty">No hay noticias nuevas relevantes para el {fecha_hoy}.</p>
        """

    return f"""
    <h2>Energiminas</h2>
    <p>Noticias nuevas del sector eléctrico: <b>{len(nuevas)}</b></p>
    {_df_to_table(nuevas, cols)}
    """


def _section_osinergmin(df: pd.DataFrame, fecha_hoy: str) -> str:
    if df.empty:
        return f"""
        <h2>Osinergmin — Normas Legales</h2>
        <p class="empty">No se encontraron datos para el {fecha_hoy}.</p>
        """

    cols = ["res", "fecha_pub", "sumilla", "link"]
    return f"""
    <h2>Osinergmin — Normas Legales</h2>
    <p>Registros extraídos: <b>{len(df)}</b></p>
    {_df_to_table(df, cols)}
    """


def _section_minem(df: pd.DataFrame, fecha_hoy: str) -> str:
    if df.empty:
        return f"""
        <h2>MINEM — Normas y Documentos</h2>
        <p class="empty">No se encontraron datos para el {fecha_hoy}.</p>
        """

    df = df.copy()
    df["relevante_bin"] = df.get("relevante", pd.Series(0, index=df.index)).apply(_to_bin)
    relev = df[df["relevante_bin"] == 1]
    resto = df[df["relevante_bin"] == 0]
    cols  = ["res", "fecha_pub", "sumilla", "link"]

    partes = [f"<h2>MINEM — Normas y Documentos</h2>"]
    partes.append(
        f"<p>Total registros: <b>{len(df)}</b> &nbsp;|&nbsp; Relevantes: <b>{len(relev)}</b></p>"
    )
    if not relev.empty:
        partes.append("<h3>Normas relevantes (sector eléctrico)</h3>")
        partes.append(_df_to_table(relev, cols))
    if not resto.empty:
        partes.append("<h3>Otras normas</h3>")
        partes.append(_df_to_table(resto, cols))
    return "\n".join(partes)


def build_html(
    df_elp: pd.DataFrame,
    df_enm: pd.DataFrame,
    df_osn: pd.DataFrame,
    df_minem: pd.DataFrame,
    fecha_hoy: str,
) -> str:
    return f"""
    <!DOCTYPE html>
    <html lang="es">
    <head><meta charset="utf-8">{_CSS}</head>
    <body>
    <p>Hola,</p>
    <p>A continuación el <b>resumen diario de normas y noticias</b> del sector eléctrico
       para el <b>{fecha_hoy}</b>:</p>

    {_section_el_peruano(df_elp, fecha_hoy)}
    <hr>
    {_section_energiminas(df_enm, fecha_hoy)}
    <hr>
    {_section_osinergmin(df_osn, fecha_hoy)}
    <hr>
    {_section_minem(df_minem, fecha_hoy)}

    <p>Saludos,<br><b>{FIRMA}</b></p>
    </body>
    </html>
    """


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA PRINCIPAL
# ─────────────────────────────────────────────

def build_and_send(destinatarios: Optional[List[str]] = None) -> None:
    now         = datetime.now(TZ)
    date_file   = now.strftime("%Y_%m_%d")   # para glob: 2026_04_06
    date_hoy    = now.strftime("%d/%m/%Y")   # para el cuerpo: 06/04/2026
    date_asunto = now.strftime("%Y-%m-%d")   # para el asunto: 2026-04-06

    log.info("Cargando JSONs de hoy (%s)…", date_file)
    df_elp   = _load_today_json("el_peruano",  date_file)
    df_enm   = _load_today_json("energiminas", date_file)
    df_osn   = _load_today_json("osinergmin",  date_file)
    df_minem = _load_today_json("minem",       date_file)

    log.info(
        "Registros cargados — El Peruano: %d | Energiminas: %d | Osinergmin: %d | MINEM: %d",
        len(df_elp), len(df_enm), len(df_osn), len(df_minem),
    )

    html    = build_html(df_elp, df_enm, df_osn, df_minem, date_hoy)
    asunto  = f"{date_asunto} | Resumen diario — Normas y Noticias Eléctricas"

    if destinatarios is None:
        env_dest = os.getenv("DESTINATARIOS", "")
        destinatarios = (
            [e.strip() for e in env_dest.split(",") if e.strip()]
            if env_dest
            else _DEFAULT_DESTINATARIOS
        )

    log.info("Autenticando Gmail…")
    service = gmail_service()
    for correo in destinatarios:
        _send_html(service, correo, asunto, html)

    log.info("✓ Reporte enviado a %d destinatario(s).", len(destinatarios))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )
    build_and_send()
