"""
pipeline.py — Orquestador principal del pipeline IEOD.
Descarga el Excel, extrae todas las hojas, guarda JSON y sube a PostgreSQL.

Por defecto procesa AYER. Este es el punto de entrada que corre GitHub Actions
todos los días a las 9 AM Lima.

Uso:
    python pipeline.py                                               # ayer
    python pipeline.py --fecha 2026-04-09                           # fecha específica
    python pipeline.py --fecha-inicio 2026-04-01 --fecha-fin 2026-04-09   # rango
    python pipeline.py --fecha 2026-04-09 --solo-json               # sin subir a PostgreSQL
"""

import argparse
import logging
import sys
from datetime import date, timedelta

from descarga_ieod import descargar_ieod
from extraccion_ieod import (
    extraer_despacho, extraer_eventos, extraer_restric_ope, extraer_mantenimiento,
    extraer_demanda_areas, extraer_princip_caudales, extraer_princip_volumenes,
    extraer_consumo_comb, extraer_disponibilidad_gas, extraer_interconexiones,
    extraer_costo_ope_ejec,
    guardar_en_json,
)
from subir_postgres import (
    subir_despacho, subir_eventos, subir_restric_ope, subir_mantenimiento,
    subir_demanda_areas, subir_princip_caudales, subir_princip_volumenes,
    subir_consumo_comb, subir_disponibilidad_gas, subir_interconexiones,
    subir_costo_ope_ejec,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def parse_args():
    p = argparse.ArgumentParser(
        description="Pipeline IEOD: descarga + extracción + carga a PostgreSQL.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--fecha", default=None, help="Fecha YYYY-MM-DD (default: ayer)")
    p.add_argument("--fecha-inicio", default=None, help="Inicio de rango YYYY-MM-DD")
    p.add_argument("--fecha-fin", default=None, help="Fin de rango YYYY-MM-DD")
    p.add_argument(
        "--solo-json",
        action="store_true",
        help="Genera el JSON pero no sube a PostgreSQL.",
    )
    return p.parse_args()


def resolver_fechas(args) -> list[date]:
    if args.fecha_inicio and args.fecha_fin:
        inicio = date.fromisoformat(args.fecha_inicio)
        fin = date.fromisoformat(args.fecha_fin)
        return [inicio + timedelta(days=i) for i in range((fin - inicio).days + 1)]
    if args.fecha:
        return [date.fromisoformat(args.fecha)]
    return [date.today() - timedelta(days=1)]


_HOJAS = [
    (extraer_despacho,           subir_despacho,           "despacho_ejecutado"),
    (extraer_eventos,            subir_eventos,            "eventos"),
    (extraer_restric_ope,        subir_restric_ope,        "restric_ope"),
    (extraer_mantenimiento,      subir_mantenimiento,      "mantenimiento_ejecutados"),
    (extraer_demanda_areas,      subir_demanda_areas,      "demanda_areas"),
    (extraer_princip_caudales,   subir_princip_caudales,   "princip_caudales"),
    (extraer_princip_volumenes,  subir_princip_volumenes,  "princip_volumenes"),
    (extraer_consumo_comb,       subir_consumo_comb,       "consumo_comb"),
    (extraer_disponibilidad_gas, subir_disponibilidad_gas, "disponibilidad_gas"),
    (extraer_interconexiones,    subir_interconexiones,    "interconexiones"),
    (extraer_costo_ope_ejec,     subir_costo_ope_ejec,     "costo_ope_ejec"),
]


def procesar_fecha(fecha: date, solo_json: bool) -> bool:
    """Ejecuta el pipeline completo para una fecha. Retorna True si al menos una hoja tuvo éxito."""
    log.info("=== Procesando: %s ===", fecha)

    # 1. Descargar Excel
    try:
        ruta_excel = descargar_ieod(fecha)
    except Exception as exc:
        log.error("[%s] Error descargando: %s", fecha, exc)
        return False

    # 2. Extraer, guardar JSON y subir a PostgreSQL para cada hoja
    exito = False
    for fn_extraer, fn_subir, clave in _HOJAS:
        try:
            recs = fn_extraer(ruta_excel, fecha)
        except Exception as exc:
            log.error("[%s] Error extrayendo %s: %s", fecha, clave, exc)
            continue

        if not recs:
            log.warning("[%s] Sin registros en %s.", fecha, clave)
            continue

        guardar_en_json(recs, fecha, clave)

        if not solo_json:
            try:
                fn_subir(recs)
            except Exception as exc:
                log.error("[%s] Error subiendo %s a PostgreSQL: %s", fecha, clave, exc)
                continue

        exito = True

    if solo_json and exito:
        log.info("[%s] --solo-json activo: omitiendo PostgreSQL.", fecha)

    return exito


def main():
    args = parse_args()
    fechas = resolver_fechas(args)
    log.info("Fechas a procesar: %s", [str(f) for f in fechas])

    errores = sum(1 for f in fechas if not procesar_fecha(f, args.solo_json))

    if errores:
        log.error("%d fecha(s) con error.", errores)
        sys.exit(1)
    log.info("=== Pipeline completado exitosamente ===")


if __name__ == "__main__":
    main()
