"""
descarga_ieod.py — Descarga el Excel IEOD diario de COES.

Puede ejecutarse directamente o importarse como módulo desde los orquestadores.

Uso directo:
    python descarga_ieod.py                                      # ayer
    python descarga_ieod.py --fecha 2026-04-09                   # fecha específica
    python descarga_ieod.py --fecha-inicio 2026-04-01 --fecha-fin 2026-04-09
"""

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import requests

import config  # credenciales y IEOD_BASE_DIR centralizados

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# URL de descarga COES
# Estructura: .../IEOD/{año}/{MM}_{MesES}/{DD}/AnexoA_{DD}{MM}.xlsx
# ---------------------------------------------------------------------------

MESES_ES = {
    1: "Enero",    2: "Febrero",   3: "Marzo",      4: "Abril",
    5: "Mayo",     6: "Junio",     7: "Julio",       8: "Agosto",
    9: "Setiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}

COES_BASE = (
    "https://www.coes.org.pe/portal/browser/download"
    "?url=Post%20Operaci%C3%B3n%2FReportes%2FIEOD"
)


def construir_url_coes(fecha: date) -> str:
    mes_nombre = MESES_ES[fecha.month]
    return (
        f"{COES_BASE}"
        f"%2F{fecha.year}"
        f"%2F{fecha.month:02d}_{mes_nombre}"
        f"%2F{fecha.day:02d}"
        f"%2FAnexoA_{fecha.day:02d}{fecha.month:02d}.xlsx"
    )


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------

def carpeta_anio(fecha: date) -> Path:
    """Retorna (y crea si no existe) {IEOD_BASE_DIR}/{año}/"""
    carpeta = Path(config.IEOD_BASE_DIR) / str(fecha.year)
    carpeta.mkdir(parents=True, exist_ok=True)
    return carpeta


def ruta_xlsx(fecha: date) -> Path:
    return carpeta_anio(fecha) / f"{fecha.isoformat()}.xlsx"


# ---------------------------------------------------------------------------
# Descarga
# ---------------------------------------------------------------------------

def descargar_ieod(fecha: date) -> Path:
    """
    Descarga el XLSX de COES y lo guarda en {IEOD_BASE_DIR}/{año}/{fecha}.xlsx.
    Si el archivo ya existe y es válido, lo reutiliza sin volver a descargar.
    """
    destino = ruta_xlsx(fecha)

    if destino.exists():
        content = destino.read_bytes()
        if len(content) >= 4 and content[:2] == b"PK":
            log.info("Excel ya existe, reutilizando: %s", destino)
            return destino
        log.warning("Archivo existente inválido (%d bytes), re-descargando.", len(content))
        destino.unlink()

    url = construir_url_coes(fecha)
    log.info("Descargando: %s", url)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()

    if "html" in resp.headers.get("Content-Type", ""):
        raise ValueError(f"COES devolvió HTML en vez de Excel. URL: {url}")

    if len(resp.content) < 4 or resp.content[:2] != b"PK":
        raise ValueError(
            f"COES devolvió un archivo inválido o vacío ({len(resp.content)} bytes). "
            f"Posibles causas: URL incorrecta, aún no publicado, o se requiere sesión. "
            f"URL: {url}"
        )

    destino.write_bytes(resp.content)
    log.info("Guardado: %s (%.1f KB)", destino, destino.stat().st_size / 1024)
    return destino


# ---------------------------------------------------------------------------
# CLI directo
# ---------------------------------------------------------------------------

def _parse_args():
    p = argparse.ArgumentParser(
        description="Descarga el Excel IEOD de COES para una o varias fechas."
    )
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
            descargar_ieod(_fecha)
        except Exception as exc:
            log.error("[%s] Error descargando: %s", _fecha, exc)
            _errores += 1
    sys.exit(1 if _errores else 0)
