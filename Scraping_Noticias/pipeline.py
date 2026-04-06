"""
pipeline.py — Pipeline de scraping de noticias y normas legales del sector eléctrico peruano.

Fuentes:
  1. Osinergmin (gob.pe)   — Normas legales con sumilla
  2. El Peruano (AJAX)     — Normas del diario oficial + clasificación de impacto (OpenAI)
  3. Energiminas (RSS)     — Noticias del sector + resumen (OpenAI)
  4. MINEM (gob.pe)        — Normas legales con sumilla

Uso:
  python pipeline.py [--fuentes osinergmin el_peruano energiminas minem] [--max-pages N]

Variables de entorno:
  OPENAI_API_KEY   — Clave de API de OpenAI (requerida para El Peruano y Energiminas)
  BASE_DATA_DIR    — Directorio raíz donde se guardan los JSON (opcional, ver CONFIG)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, quote, urlencode, urljoin, urlparse, urlunparse
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup
from lxml import html as lxml_html

# ─────────────────────────────────────────────
# CONFIGURACIÓN GLOBAL
# ─────────────────────────────────────────────

# Directorio raíz para todos los JSON de salida.
# Se puede sobreescribir con la variable de entorno BASE_DATA_DIR.
_DEFAULT_BASE = (
    r"C:\Users\egaray\Macroconsult S.A\Soporte TI - REGCOM"
    r"\0_DATA\0.1_ELECTRICIDAD\0_BASES DE DATOS\Normas"
)
BASE_DATA_DIR = Path(os.getenv("BASE_DATA_DIR", _DEFAULT_BASE))

OUTDIRS: Dict[str, Path] = {
    "osinergmin": BASE_DATA_DIR / "Osinergmin_gob",
    "el_peruano": BASE_DATA_DIR / "El Peruano",
    "energiminas": BASE_DATA_DIR / "Energiminas",
    "minem":       BASE_DATA_DIR / "MINEM",
}

# Directorio de logs (relativo a este script)
LOGS_DIR = Path(__file__).parent / "logs"

# Clave OpenAI (usada por El Peruano y Energiminas)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

# Zona horaria de referencia
LIMA_TZ = ZoneInfo("America/Lima")

# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────

def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(LIMA_TZ).strftime("%Y_%m_%d")
    log_file = LOGS_DIR / f"{today}_pipeline.log"

    fmt = "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    return logging.getLogger("pipeline")


logger = setup_logging()

# ─────────────────────────────────────────────
# UTILIDADES COMUNES
# ─────────────────────────────────────────────

MONTHS_ES = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "setiembre": 9, "septiembre": 9,
    "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def parse_fecha_es(fecha_texto: str) -> str:
    """'21 de enero de 2026' → '21/01/2026'."""
    t = clean_text(fecha_texto).lower()
    # Normalizar tildes
    for src, dst in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u")]:
        t = t.replace(src, dst)
    m = re.search(r"(\d{1,2})\s*(?:de)?\s*([a-z]+)\s*(?:de)?\s*(\d{4})", t)
    if not m:
        return ""
    dd, mes, yyyy = int(m.group(1)), m.group(2), int(m.group(3))
    mm = MONTHS_ES.get(mes, 0)
    return f"{dd:02d}/{mm:02d}/{yyyy:04d}" if mm else ""


def parse_ddmmyyyy(raw: str) -> str:
    """Extrae DD/MM/YYYY de cualquier cadena."""
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", raw or "")
    return f"{m.group(1)}/{m.group(2)}/{m.group(3)}" if m else clean_text(raw)


def build_session(source: str = "") -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/124.0 Safari/537.36 ({source}Scraper/1.0)"
        ),
        "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


def get_html(session: requests.Session, url: str, retries: int = 3,
             backoff: float = 1.5, timeout: int = 30) -> str:
    last_err: Exception = RuntimeError("sin intentos")
    for i in range(retries):
        try:
            r = session.get(url, timeout=timeout)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(backoff * (i + 1))
    raise RuntimeError(f"No se pudo obtener {url} — {last_err}")


def save_json(data: list, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def today_filename(prefix: str, ext: str = "json") -> str:
    return datetime.now(LIMA_TZ).strftime(f"%Y_%m_%d_{prefix}.{ext}")


# ─────────────────────────────────────────────
# OPENAI HELPERS
# ─────────────────────────────────────────────

def _openai_client():
    """Crea un cliente OpenAI. Lanza error descriptivo si no hay API key."""
    if not OPENAI_API_KEY:
        raise EnvironmentError(
            "OPENAI_API_KEY no está definida. "
            "Exporta la variable de entorno antes de ejecutar el pipeline."
        )
    from openai import OpenAI
    return OpenAI(api_key=OPENAI_API_KEY)


def _chat_json(client, system_prompt: str, user_prompt: str,
               max_tokens: int = 300, retries: int = 3) -> dict:
    last_err: Exception = RuntimeError("sin intentos")
    for attempt in range(1, retries + 1):
        try:
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=max_tokens,
            )
            return json.loads(resp.choices[0].message.content)
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(2.0 * attempt)
    raise RuntimeError(f"OpenAI falló tras {retries} intentos: {last_err}")


# ─────────────────────────────────────────────
# SCRAPER 1 — OSINERGMIN
# ─────────────────────────────────────────────

def scrape_osinergmin(max_pages: Optional[int] = 3, delay: float = 0.7) -> List[dict]:
    log = logging.getLogger("pipeline.osinergmin")
    BASE = "https://www.gob.pe/institucion/osinergmin/normas-legales"

    LIST_ITEMS = "#main section ul > li"
    ITEM_LINK  = "div div div.col-md-9 div.flex.items-center a"
    ITEM_DATE  = "div div div.col-md-9 time"
    DETAIL_SEL = (
        "#main > div > div > div > div > article > section "
        "> div > div > div > div:nth-child(1)"
    )

    def _next_url(soup: BeautifulSoup, current: str) -> Optional[str]:
        a = soup.select_one('a[rel="next"]')
        if a and a.get("href"):
            return urljoin(current, a["href"])
        for cand in soup.select("a"):
            if clean_text(cand.get_text()).lower() in ("siguiente", "next", "→"):
                href = cand.get("href")
                if href:
                    return urljoin(current, href)
        return None

    def _with_page(url: str, n: int) -> str:
        parts = list(urlparse(url))
        q = parse_qs(parts[4])
        q["page"] = [str(n)]
        parts[4] = urlencode(q, doseq=True)
        return urlunparse(parts)

    session = build_session("Osinergmin")
    results: List[dict] = []
    seen: set = set()
    current_url = BASE
    page = 1

    while True:
        if max_pages is not None and page > max_pages:
            break

        log.info("Página %d: %s", page, current_url)
        try:
            html_text = get_html(session, current_url)
        except Exception as e:
            log.error("Error en página %d: %s", page, e)
            break

        soup = BeautifulSoup(html_text, "html.parser")
        lis  = soup.select(LIST_ITEMS)

        if not lis:
            if "page=" not in current_url:
                current_url = _with_page(BASE, page)
                continue
            break

        for li in lis:
            a = li.select_one(ITEM_LINK)
            t = li.select_one(ITEM_DATE)
            if not a or not a.get("href"):
                continue

            link = urljoin(current_url, a["href"])
            if link in seen:
                continue
            seen.add(link)

            res       = clean_text(a.get_text())
            fecha_raw = clean_text(t.get_text()) if t else ""
            fecha_pub = parse_fecha_es(fecha_raw)

            time.sleep(delay)
            sumilla = ""
            try:
                detail_html = get_html(session, link)
                d_soup = BeautifulSoup(detail_html, "html.parser")
                node   = d_soup.select_one(DETAIL_SEL)
                if node:
                    sumilla = clean_text(node.get_text(" "))
                else:
                    article = d_soup.select_one("#main article")
                    if article:
                        sumilla = clean_text(article.get_text(" "))[:600]
            except Exception as e:
                log.warning("Error en detalle %s: %s", link, e)

            results.append({
                "fuente":    "Osinergmin_gob",
                "link":      link,
                "res":       res,
                "fecha_pub": fecha_pub,
                "sumilla":   sumilla,
            })

        next_url = _next_url(soup, current_url)
        if next_url:
            current_url = next_url
            page += 1
            continue

        page += 1
        current_url = _with_page(BASE, page)

    log.info("Total Osinergmin: %d registros", len(results))
    return results


# ─────────────────────────────────────────────
# SCRAPER 2 — EL PERUANO
# ─────────────────────────────────────────────

_EP_RELEVANCE_SUBSTRINGS = ["MINEM", "-OS/"]
_EP_RELEVANCE_REGEX = []          # añadir patrones regex si se necesitan
_EP_EXCLUDE_SUBSTRINGS: List[str] = []

_EP_IMPACT_PROMPT = """
Eres un analista regulatorio experto en el sector eléctrico peruano.
Evalúa si la SUMILLA describe una norma con impacto IMPORTANTE en el sector eléctrico.

Considera IMPORTANTE (impacto=1) si:
- Modifica Procedimientos Técnicos del COES o reglas de operación/seguridad del SEIN.
- Modifica metodologías, procedimientos, parámetros o regulación tarifaria del OSINERGMIN.
- Modifica reglamentos del sector eléctrico (generación, transmisión, distribución).
- Otorga, amplía o modifica concesiones eléctricas, servidumbres o permisos críticos.
- Impone nuevas obligaciones regulatorias/operativas a agentes del sector.
- Afecta la planificación/expansión/seguridad del sistema o el despacho.
- Cambios en hidrocarburos con impacto directo en la operación eléctrica.

NO es IMPORTANTE (impacto=0) si:
- Son designaciones, viajes, reconocimientos o asuntos administrativos sin efecto técnico.
- No afecta reglas, tarifas, concesiones ni operación del sistema eléctrico.
- Modifica derechos de servidumbre solo para estudios.
- Temas administrativos sobre cableados.

Devuelve EXCLUSIVAMENTE: {"impacto": 1} o {"impacto": 0}

Nro/Tipo de resolución: {res}
SUMILLA:
\"\"\"{sumilla}\"\"\"
"""


def _ep_is_relevant(res_text: str) -> str:
    t = (res_text or "").upper()
    for excl in _EP_EXCLUDE_SUBSTRINGS:
        if excl.upper() in t:
            return "0"
    for inc in _EP_RELEVANCE_SUBSTRINGS:
        if inc.upper() in t:
            return "1"
    for pat in _EP_RELEVANCE_REGEX:
        if re.search(pat, res_text, re.IGNORECASE):
            return "1"
    return "0"


def _ep_classify_impact(client, sumilla: str, res: str) -> Optional[int]:
    prompt = _EP_IMPACT_PROMPT.format(sumilla=sumilla[:2000], res=res)
    try:
        obj = _chat_json(
            client,
            system_prompt="Responde únicamente con el JSON solicitado.",
            user_prompt=prompt,
            max_tokens=20,
        )
        val = obj.get("impacto")
        return int(val) if val is not None else None
    except Exception as e:
        logging.getLogger("pipeline.el_peruano").warning("Error clasificando impacto: %s", e)
        return None


def scrape_el_peruano(
    use_date_filter: bool = False,
    from_date: str = "",
    to_date: str = "",
    solo_extra: bool = False,
    classify_impact: bool = True,
) -> List[dict]:
    log = logging.getLogger("pipeline.el_peruano")
    BASE = "https://diariooficial.elperuano.pe"
    LIST_PAGE = f"{BASE}/normas"
    TIMEOUT = 25

    session = build_session("ElPeruano")
    session.headers["X-Requested-With"] = "XMLHttpRequest"
    session.headers["Referer"]          = LIST_PAGE

    def _get_initial() -> str:
        r = session.get(f"{BASE}/Normas/LoadNormasLegales?Length=0", timeout=TIMEOUT)
        r.raise_for_status()
        return r.text

    def _ddmmyyyy_to_mdy(d: str) -> str:
        dd, mm, yyyy = d.split("/")
        return f"{mm}/{dd}/{yyyy} 00:00:00"

    def _get_filtered(desde: str, hasta: str, extra: bool) -> str:
        url = f"{BASE}/Normas/Filtro?dateparam={quote(_ddmmyyyy_to_mdy(desde))}"
        data: dict = {"cddesde": desde, "cdhasta": hasta}
        if extra:
            data["tipo"] = "on"
        r = session.post(url, data=data, timeout=TIMEOUT,
                         headers={"Origin": BASE})
        r.raise_for_status()
        return r.text

    def _parse_articles(fragment: str) -> List[dict]:
        doc      = lxml_html.fromstring(fragment)
        articles = doc.xpath("//article")
        records  = []
        for art in articles:
            a_el = art.xpath(".//h5//a") or art.xpath(
                ".//a[contains(@href,'Normas/obtenerDocumento') or contains(@href,'/Normas/')]"
            )
            if not a_el:
                continue
            a          = a_el[0]
            link       = urljoin(BASE, a.get("href") or "")
            res_text   = clean_text(a.text_content())

            fecha_raw  = " ".join(clean_text(x) for x in art.xpath(".//p//b//text()") if clean_text(x))
            fecha_pub  = parse_ddmmyyyy(fecha_raw)

            sumilla_tx = art.xpath(".//p[2]//text()") or art.xpath(
                ".//p[not(contains(.,'Fecha'))][1]//text()"
            )
            sumilla    = clean_text(" ".join(sumilla_tx)) if sumilla_tx else ""

            records.append({
                "fuente":    "El Peruano",
                "link":      link,
                "res":       res_text,
                "fecha_pub": fecha_pub,
                "sumilla":   sumilla,
                "relevante": _ep_is_relevant(res_text),
                "impacto":   None,
            })
        return records

    # Obtener listado
    try:
        if use_date_filter and from_date and to_date:
            log.info("Usando filtro de fechas: %s — %s", from_date, to_date)
            fragment = _get_filtered(from_date, to_date, solo_extra)
        else:
            log.info("Cargando lista inicial (sin filtro de fechas)")
            fragment = _get_initial()
    except Exception as e:
        log.error("Error obteniendo lista El Peruano: %s", e)
        return []

    items = _parse_articles(fragment)
    log.info("El Peruano: %d normas extraídas", len(items))

    # Clasificar impacto con OpenAI (sólo relevantes)
    if classify_impact and OPENAI_API_KEY:
        try:
            client = _openai_client()
            relevant = [i for i in items if i["relevante"] == "1"]
            log.info("Clasificando impacto para %d normas relevantes…", len(relevant))
            for item in relevant:
                item["impacto"] = _ep_classify_impact(client, item["sumilla"], item["res"])
                time.sleep(0.3)
        except Exception as e:
            log.warning("No se pudo clasificar impacto (OpenAI): %s", e)
    elif classify_impact and not OPENAI_API_KEY:
        log.warning("OPENAI_API_KEY no definida — se omite clasificación de impacto.")

    return items


# ─────────────────────────────────────────────
# SCRAPER 3 — ENERGIMINAS
# ─────────────────────────────────────────────

_ENM_RELEVANCE = ["ELECTRICIDAD","DGE","RURAL","CONCESION","COES","SEIN",
                  "TRANSMISION","DISTRIBUCION","GENERACION","RENOVABLE","SOLAR","EOLICA"]
_ENM_EXCLUDE   = ["MINERIA","PETROLEO","GAS","HIDROCARBUROS"]

_ENM_RESUMEN_SYS = (
    "Eres un analista senior. Resume la noticia en máximo 5 ideas principales.\n"
    "Devuelve EXCLUSIVAMENTE un JSON: {\"resumen\": [\"idea 1\", ...]}\n"
    "Cada idea: ≤25 palabras. No inventes datos."
)
_ENM_RELEVANCIA_SYS = (
    "Actúa como analista senior del sector eléctrico.\n"
    "Clasifica la noticia como RELEVANTE (1) o NO RELEVANTE (0).\n"
    "Es NO RELEVANTE (0) solo si trata principalmente de:\n"
    "  (a) un EVENTO (convocatoria, agenda, ponentes, inscripción), o\n"
    "  (b) un NOMBRAMIENTO/DESIGNACIÓN de funcionario público.\n"
    "Devuelve EXCLUSIVAMENTE: {\"relevante\": 0} o {\"relevante\": 1}"
)


def _enm_is_relevant_kw(titulo: str, contenido: str = "") -> str:
    text = (titulo + " " + contenido).upper()
    for excl in _ENM_EXCLUDE:
        if excl in text and "ELECTRICIDAD" not in text:
            pass
    for inc in _ENM_RELEVANCE:
        if inc in text:
            return "1"
    return "0"


def _enm_crawl_article(url: str, session: requests.Session) -> str:
    try:
        time.sleep(0.5)
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        div  = soup.find("div", class_="elementor-widget-theme-post-content")
        if div:
            return "\n\n".join(
                clean_text(p.get_text())
                for p in div.find_all("p")
                if len(p.get_text()) > 5
            )
        std = soup.find("div", class_="entry-content")
        if std:
            return clean_text(std.get_text())
    except Exception as e:
        logging.getLogger("pipeline.energiminas").warning("Error crawling %s: %s", url, e)
    return ""


def _enm_parse_rss_date(date_str: str) -> str:
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str.replace(" +0000", ""), "%a, %d %b %Y %H:%M:%S")
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return date_str[:10]


def scrape_energiminas(target_count: int = 15, summarize: bool = True) -> List[dict]:
    log = logging.getLogger("pipeline.energiminas")
    RSS_BASE = "https://energiminas.com/category/electricidad/feed/"
    OUTDIR_ENM = OUTDIRS["energiminas"]

    session = build_session("Energiminas")
    pages_needed = (target_count // 10) + 1
    items: List[dict] = []

    for p in range(1, pages_needed + 1):
        if len(items) >= target_count:
            break
        url = RSS_BASE if p == 1 else f"{RSS_BASE}?paged={p}"
        log.info("RSS página %d: %s", p, url)
        try:
            r = session.get(url, timeout=10)
            if r.status_code != 200:
                log.warning("RSS página %d status %d", p, r.status_code)
                continue
            soup = BeautifulSoup(r.content, "xml")
            for item in soup.find_all("item"):
                if len(items) >= target_count:
                    break
                titulo    = item.title.get_text() if item.title else ""
                link      = item.link.get_text() if item.link else ""
                fecha_pub = _enm_parse_rss_date(item.pubDate.get_text() if item.pubDate else "")

                contenido = _enm_crawl_article(link, session)
                if not contenido and item.description:
                    contenido = clean_text(
                        BeautifulSoup(item.description.get_text(), "html.parser").get_text()
                    )

                items.append({
                    "fuente":        "Revista Energiminas",
                    "link":          link,
                    "titular":       titulo,
                    "fecha_pub":     fecha_pub,
                    "contenido":     contenido,
                    "relevante":     _enm_is_relevant_kw(titulo, contenido),
                    "resumen":       [],
                    "nueva_noticia": 0,
                })
        except Exception as e:
            log.error("Error RSS página %d: %s", p, e)

    log.info("Energiminas: %d noticias extraídas", len(items))

    # Detectar nuevas noticias vs. JSON de ayer
    today_d    = datetime.now(LIMA_TZ).date()
    ayer_d     = today_d - timedelta(days=1)
    today_str  = today_d.strftime("%d/%m/%Y")
    ayer_str   = ayer_d.strftime("%d/%m/%Y")
    ayer_name  = ayer_d.strftime("%Y_%m_%d_Energiminas.json")
    ayer_path  = OUTDIR_ENM / ayer_name

    titulos_ayer: set = set()
    if ayer_path.exists():
        try:
            with open(ayer_path, encoding="utf-8") as f:
                data_ayer = json.load(f)
            titulos_ayer = {clean_text(x.get("titular", "")).lower() for x in data_ayer}
        except Exception as e:
            log.warning("No se pudo leer JSON de ayer (%s): %s", ayer_path, e)

    candidatas = [
        it for it in items
        if it["fecha_pub"] in (today_str, ayer_str)
        and clean_text(it["titular"]).lower() not in titulos_ayer
    ]
    titulos_nuevas = {clean_text(it["titular"]).lower() for it in candidatas}
    for it in items:
        if clean_text(it["titular"]).lower() in titulos_nuevas:
            it["nueva_noticia"] = 1

    # Resumir + reclasificar con OpenAI
    if summarize and candidatas:
        if not OPENAI_API_KEY:
            log.warning("OPENAI_API_KEY no definida — se omite resumen/relevancia con IA.")
        else:
            try:
                client = _openai_client()
                log.info("Resumiendo %d noticias nuevas con OpenAI…", len(candidatas))
                for it in candidatas:
                    bloque = (
                        f"TITULAR: {it['titular']}\n"
                        f"FECHA: {it['fecha_pub']}\n"
                        f"LINK: {it['link']}\n\n"
                        f"CONTENIDO:\n{it['contenido'][:9000]}"
                    )
                    try:
                        res_json = _chat_json(client, _ENM_RESUMEN_SYS, bloque, max_tokens=300)
                        it["resumen"] = [clean_text(str(x)) for x in res_json.get("resumen", [])][:5]
                        rel_json = _chat_json(client, _ENM_RELEVANCIA_SYS, bloque, max_tokens=20)
                        it["relevante"] = "1" if int(rel_json.get("relevante", 1)) == 1 else "0"
                        time.sleep(0.3)
                    except Exception as e:
                        log.warning("Error procesando '%s': %s", it["titular"][:60], e)
            except Exception as e:
                log.warning("OpenAI no disponible para Energiminas: %s", e)

    return items


# ─────────────────────────────────────────────
# SCRAPER 4 — MINEM
# ─────────────────────────────────────────────

_MINEM_RELEVANCE = ["ELECTRICIDAD","DGE","RURAL","CONCESION","COES","SEIN",
                    "TRANSMISION","DISTRIBUCION","GENERACION"]
_MINEM_EXCLUDE   = ["VACACIONES","LICENCIA","DESIGNAR","NOMBRAMIENTO","RENUNCIA","SUPLENCIA"]


def _minem_is_relevant(sumilla: str, titulo: str) -> str:
    text = (sumilla + " " + titulo).upper()
    for excl in _MINEM_EXCLUDE:
        if excl in text:
            return "0"
    for inc in _MINEM_RELEVANCE:
        if inc in text:
            return "1"
    return "0"


def _minem_get_sumilla(url: str, session: requests.Session) -> str:
    try:
        time.sleep(0.5)
        r = session.get(url, timeout=10)
        if r.status_code != 200:
            return ""
        soup = BeautifulSoup(r.text, "html.parser")
        div  = soup.find("div", class_="description")
        if div:
            text = div.get_text(" ", strip=True)
            if "Esta norma pertenece" in text:
                text = text.split("Esta norma pertenece")[0]
            return clean_text(text)
    except Exception as e:
        logging.getLogger("pipeline.minem").warning("Error sumilla %s: %s", url, e)
    return ""


def scrape_minem(pages: int = 1) -> List[dict]:
    log = logging.getLogger("pipeline.minem")
    BASE      = "https://www.gob.pe"
    START_URL = "https://www.gob.pe/institucion/minem/normas-legales"

    session = build_session("MINEM")
    items: List[dict] = []

    for page in range(1, pages + 1):
        url = f"{START_URL}?sheet={page}"
        log.info("Página %d: %s", page, url)
        try:
            r    = session.get(url, timeout=15)
            soup = BeautifulSoup(r.text, "html.parser")
            tags = soup.find_all("a", class_="card__mock")
            if not tags:
                log.info("No se encontraron normas en página %d, deteniendo.", page)
                break
            log.info("Página %d: %d normas encontradas", page, len(tags))
            for i, tag in enumerate(tags, 1):
                res_title = clean_text(tag.get_text())
                href      = tag.get("href", "")
                link      = urljoin(BASE, href)

                fecha_clean = ""
                time_tag = tag.find_next("time")
                if time_tag and time_tag.has_attr("datetime"):
                    try:
                        dt = datetime.strptime(time_tag["datetime"][:10], "%Y-%m-%d")
                        fecha_clean = dt.strftime("%d/%m/%Y")
                    except Exception:
                        fecha_clean = clean_text(time_tag.get_text())

                log.debug("  [%d/%d] %s", i, len(tags), res_title[:60])
                sumilla = _minem_get_sumilla(link, session) or res_title

                items.append({
                    "fuente":    "MINEM-Normas y Documentos",
                    "link":      link,
                    "res":       res_title,
                    "fecha_pub": fecha_clean,
                    "sumilla":   sumilla,
                    "relevante": _minem_is_relevant(sumilla, res_title),
                })
        except Exception as e:
            log.error("Error en página %d: %s", page, e)

    log.info("MINEM: %d registros", len(items))
    return items


# ─────────────────────────────────────────────
# PIPELINE PRINCIPAL
# ─────────────────────────────────────────────

SCRAPERS = {
    "osinergmin": {
        "fn":   scrape_osinergmin,
        "key":  "osinergmin",
        "name": "Osinergmin_gob",
    },
    "el_peruano": {
        "fn":   scrape_el_peruano,
        "key":  "el_peruano",
        "name": "EL_Peruano",
    },
    "energiminas": {
        "fn":   scrape_energiminas,
        "key":  "energiminas",
        "name": "Energiminas",
    },
    "minem": {
        "fn":   scrape_minem,
        "key":  "minem",
        "name": "MINEM",
    },
}


def run_pipeline(
    fuentes: Optional[List[str]] = None,
    max_pages: int = 3,
) -> Dict[str, dict]:
    """
    Ejecuta los scrapers indicados en orden y guarda los JSON.

    Returns:
        Diccionario con el resumen de ejecución por fuente:
        {
          "osinergmin": {"status": "ok", "count": 75, "path": "..."},
          ...
        }
        Útil para generar reportes / enviar por correo.
    """
    if fuentes is None:
        fuentes = list(SCRAPERS.keys())

    report: Dict[str, dict] = {}

    for key in fuentes:
        if key not in SCRAPERS:
            logger.warning("Fuente desconocida: '%s' — se omite.", key)
            continue

        cfg = SCRAPERS[key]
        logger.info("=" * 60)
        logger.info("INICIANDO: %s", cfg["name"])
        logger.info("=" * 60)

        try:
            # Argumentos específicos por scraper
            if key == "osinergmin":
                data = cfg["fn"](max_pages=max_pages)
            elif key == "minem":
                data = cfg["fn"](pages=max_pages)
            else:
                data = cfg["fn"]()

            outdir = OUTDIRS[cfg["key"]]
            outdir.mkdir(parents=True, exist_ok=True)
            filename = today_filename(cfg["name"])
            outpath  = outdir / filename
            save_json(data, outpath)

            logger.info("✓ %s: %d registros → %s", cfg["name"], len(data), outpath)
            report[key] = {
                "status": "ok",
                "count":  len(data),
                "path":   str(outpath),
                "data":   data,
            }
        except Exception as e:
            logger.error("✗ %s: ERROR — %s", cfg.get("name", key), e, exc_info=True)
            report[key] = {
                "status": "error",
                "error":  str(e),
                "count":  0,
                "path":   "",
                "data":   [],
            }

    # Resumen final
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETADO")
    for key, r in report.items():
        if r["status"] == "ok":
            logger.info("  %-15s OK   — %d registros", key, r["count"])
        else:
            logger.info("  %-15s ERROR — %s", key, r.get("error", ""))
    logger.info("=" * 60)

    # Guardar resumen en logs
    summary_path = LOGS_DIR / today_filename("pipeline_summary")
    try:
        summary_export = {
            k: {kk: vv for kk, vv in v.items() if kk != "data"}
            for k, v in report.items()
        }
        save_json([summary_export], summary_path)
    except Exception as e:
        logger.warning("No se pudo guardar resumen: %s", e)

    return report


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pipeline de scraping de noticias y normas legales (sector eléctrico Perú)."
    )
    parser.add_argument(
        "--fuentes",
        nargs="+",
        choices=list(SCRAPERS.keys()),
        default=list(SCRAPERS.keys()),
        help="Fuentes a ejecutar (por defecto: todas).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=3,
        help="Número máximo de páginas a recorrer en Osinergmin y MINEM (por defecto: 3).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_pipeline(fuentes=args.fuentes, max_pages=args.max_pages)
