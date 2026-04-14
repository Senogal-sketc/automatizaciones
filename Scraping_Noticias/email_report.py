"""
email_report.py — Genera y envía el reporte diario de normas/noticias por Gmail.

Lee los registros NUEVOS del día directamente desde Supabase (tabla normas_noticias,
fecha_scraping = hoy). Solo envía si hay registros nuevos.

Variables de entorno:
  SUPABASE_DB_URL   — Cadena de conexión PostgreSQL de Supabase (requerida)
  GMAIL_CREDENTIALS — Path a credentials.json  (default: credentials.json)
  GMAIL_TOKEN       — Path a token.json         (default: token.json)
  DESTINATARIOS     — Emails separados por coma (sobreescribe la lista interna)
"""

from __future__ import annotations

import base64
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

import db as _db

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

TZ = ZoneInfo("America/Lima")

_DEFAULT_DESTINATARIOS = [
    "egaray@grupomacro.pe",
    "evander.garay@gmail.com",
    "ddiaz@grupomacro.pe",
    "varce@grupomacro.pe",
    "gtamayo@grupomacro.pe",
    "vflores@grupomacro.pe",
]

GMAIL_SCOPES     = ["https://www.googleapis.com/auth/gmail.send"]
CREDENTIALS_FILE = Path(os.getenv("GMAIL_CREDENTIALS", "credentials.json"))
TOKEN_FILE       = Path(os.getenv("GMAIL_TOKEN",        "token.json"))
FIRMA            = "Evander Garay"

log = logging.getLogger("email_report")

# ─────────────────────────────────────────────
# GMAIL AUTH
# ─────────────────────────────────────────────

def gmail_service():
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
                    "Colócalo junto a este script o define GMAIL_CREDENTIALS."
                )
            log.info("Iniciando flujo OAuth (solo necesario la primera vez)…")
            flow = InstalledAppFlow.from_client_secrets_file(
                str(CREDENTIALS_FILE), GMAIL_SCOPES
            )
            creds = flow.run_local_server(port=0)
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
# HELPERS HTML
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


def _to_bin(x) -> int:
    if pd.isna(x):
        return 0
    return 1 if str(x).strip().lower() in ("1", "true", "t", "yes", "y") else 0


def _link_html(url: str, texto: str = "ver") -> str:
    u = str(url or "").strip()
    return f'<a href="{u}" target="_blank" rel="noopener">{texto}</a>' if u else ""


def _resumen_html(val) -> str:
    if isinstance(val, list) and val:
        return "<ul>" + "".join(f"<li>{item}</li>" for item in val if item) + "</ul>"
    return str(val) if val else ""


def _df_to_table(df: pd.DataFrame, cols: list, link_col: str = "link") -> str:
    if df.empty:
        return '<p class="empty">Sin registros.</p>'
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
                cell = _link_html(val)
            elif c == "resumen":
                cell = _resumen_html(val)
            else:
                cell = str(val) if not pd.isna(val) else ""
            cells.append(f"<td>{cell}</td>")
        rows.append(f"<tr>{''.join(cells)}</tr>")
    return f"<table><tr>{headers}</tr>{''.join(rows)}</table>"


# ─────────────────────────────────────────────
# SECCIONES DEL CORREO
# ─────────────────────────────────────────────

def _section_el_peruano(df: pd.DataFrame, fecha_hoy: str) -> str:
    if df.empty:
        return f'<h2>El Peruano</h2><p class="empty">Sin normas nuevas para {fecha_hoy}.</p>'

    df = df.copy()
    df["relevante_bin"] = df.get("relevante", pd.Series(0, index=df.index)).apply(_to_bin)
    df["impacto_bin"]   = df.get("impacto",   pd.Series(0, index=df.index)).apply(_to_bin)
    relev = df[df["relevante_bin"] == 1]
    imp1  = relev[relev["impacto_bin"] == 1]
    imp0  = relev[relev["impacto_bin"] == 0]
    cols  = ["res", "sumilla", "link"]

    if relev.empty:
        return (f'<h2>El Peruano</h2>'
                f'<p>Total nuevas: <b>{len(df)}</b> — ninguna relevante para el sector eléctrico.</p>')

    return f"""
    <h2>El Peruano</h2>
    <p>Normas nuevas: <b>{len(df)}</b> &nbsp;|&nbsp;
       Relevantes: <b>{len(relev)}</b> &nbsp;|&nbsp;
       Con impacto eléctrico: <b>{len(imp1)}</b></p>
    <h3>Con impacto eléctrico</h3>
    {_df_to_table(imp1, cols)}
    <h3>Otras relevantes</h3>
    {_df_to_table(imp0, cols)}
    """


def _section_energiminas(df: pd.DataFrame, fecha_hoy: str) -> str:
    if df.empty:
        return f'<h2>Energiminas</h2><p class="empty">Sin noticias nuevas para {fecha_hoy}.</p>'

    df = df.copy()
    df["relevante_bin"] = df.get("relevante", pd.Series(1, index=df.index)).apply(_to_bin)
    nuevas = df[df["relevante_bin"] == 1]
    cols   = ["titular", "resumen", "link"]

    if nuevas.empty:
        return (f'<h2>Energiminas</h2>'
                f'<p>Noticias nuevas: <b>{len(df)}</b> — ninguna relevante para el sector eléctrico.</p>')

    return f"""
    <h2>Energiminas</h2>
    <p>Noticias nuevas relevantes: <b>{len(nuevas)}</b></p>
    {_df_to_table(nuevas, cols)}
    """


def _section_osinergmin(df: pd.DataFrame, fecha_hoy: str) -> str:
    if df.empty:
        return f'<h2>Osinergmin</h2><p class="empty">Sin normas nuevas para {fecha_hoy}.</p>'
    cols = ["res", "fecha_pub", "sumilla", "link"]
    return f"""
    <h2>Osinergmin — Normas Legales</h2>
    <p>Normas nuevas: <b>{len(df)}</b></p>
    {_df_to_table(df, cols)}
    """


def _section_minem(df: pd.DataFrame, fecha_hoy: str) -> str:
    if df.empty:
        return f'<h2>MINEM</h2><p class="empty">Sin normas nuevas para {fecha_hoy}.</p>'
    df = df.copy()
    df["relevante_bin"] = df.get("relevante", pd.Series(0, index=df.index)).apply(_to_bin)
    relev = df[df["relevante_bin"] == 1]
    resto = df[df["relevante_bin"] == 0]
    cols  = ["res", "fecha_pub", "sumilla", "link"]
    partes = [
        f"<h2>MINEM — Normas y Documentos</h2>",
        f"<p>Total nuevas: <b>{len(df)}</b> &nbsp;|&nbsp; Relevantes: <b>{len(relev)}</b></p>",
    ]
    if not relev.empty:
        partes += ["<h3>Relevantes (sector eléctrico)</h3>", _df_to_table(relev, cols)]
    if not resto.empty:
        partes += ["<h3>Otras normas</h3>", _df_to_table(resto, cols)]
    return "\n".join(partes)


def build_html(
    df_elp: pd.DataFrame,
    df_enm: pd.DataFrame,
    df_osn: pd.DataFrame,
    df_minem: pd.DataFrame,
    fecha_hoy: str,
) -> str:
    return f"""<!DOCTYPE html>
<html lang="es">
<head><meta charset="utf-8">{_CSS}</head>
<body>
<p>Hola,</p>
<p>Resumen de <b>normas y noticias nuevas</b> del sector eléctrico para el <b>{fecha_hoy}</b>:</p>

{_section_el_peruano(df_elp, fecha_hoy)}
<hr>
{_section_energiminas(df_enm, fecha_hoy)}
<hr>
{_section_osinergmin(df_osn, fecha_hoy)}
<hr>
{_section_minem(df_minem, fecha_hoy)}

<p>Saludos,<br><b>{FIRMA}</b></p>
</body>
</html>"""


# ─────────────────────────────────────────────
# PUNTO DE ENTRADA PRINCIPAL
# ─────────────────────────────────────────────

FUENTE_NAMES = {
    "Osinergmin_gob":        "osinergmin",
    "El Peruano":            "el_peruano",
    "Revista Energiminas":   "energiminas",
    "MINEM-Normas y Documentos": "minem",
}


def _to_df(records: list) -> pd.DataFrame:
    return pd.DataFrame(records) if records else pd.DataFrame()


def build_and_send(destinatarios: Optional[List[str]] = None) -> None:
    now         = datetime.now(TZ)
    date_hoy    = now.strftime("%d/%m/%Y")
    date_asunto = now.strftime("%Y-%m-%d")

    # Leer registros nuevos de hoy desde Supabase
    log.info("Consultando registros nuevos de hoy en Supabase…")
    all_new = _db.get_new_today()

    if not all_new:
        log.info("No hay registros nuevos hoy. No se enviará correo.")
        return

    log.info("Total registros nuevos hoy: %d", len(all_new))

    # Separar por fuente
    def _filter(fuente_val: str) -> pd.DataFrame:
        return _to_df([r for r in all_new if r.get("fuente") == fuente_val])

    df_elp   = _filter("El Peruano")
    df_enm   = _filter("Revista Energiminas")
    df_osn   = _filter("Osinergmin_gob")
    df_minem = _filter("MINEM-Normas y Documentos")

    log.info(
        "El Peruano: %d | Energiminas: %d | Osinergmin: %d | MINEM: %d",
        len(df_elp), len(df_enm), len(df_osn), len(df_minem),
    )

    html   = build_html(df_elp, df_enm, df_osn, df_minem, date_hoy)
    asunto = f"{date_asunto} | Resumen diario — Normas y Noticias Eléctricas"

    if destinatarios is None:
        env_dest = os.getenv("DESTINATARIOS", "")
        destinatarios = (
            [e.strip() for e in env_dest.split(",") if e.strip()]
            if env_dest else _DEFAULT_DESTINATARIOS
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
