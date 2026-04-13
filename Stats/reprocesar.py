"""
reprocesar.py — Re-extrae datos desde Excels ya descargados y los sube a PostgreSQL.
No realiza descargas. Útil para reprocesar fechas históricas o corregir extracciones.

Uso:
    python reprocesar.py --fecha 2026-04-09
    python reprocesar.py --fecha-inicio 2026-04-01 --fecha-fin 2026-04-09
    python reprocesar.py --fecha 2026-04-09 --solo-json
"""

import argparse
import logging
import sys
from datetime import date, timedelta

from descarga_ieod import ruta_xlsx
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
        description="Re-extrae datos IEOD desde Excels ya descargados.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--fecha", default=None, help="Fecha YYYY-MM-DD")
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
    raise ValueError("Debes especificar --fecha o --fecha-inicio + --fecha-fin.")


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
    """Re-extrae y sube datos para una fecha. Retorna True si al menos una hoja tuvo éxito."""
    log.info("=== Reprocesando: %s ===", fecha)

    ruta = ruta_xlsx(fecha)
    if not ruta.exists():
        log.error("[%s] Excel no encontrado: %s", fecha, ruta)
        return False

    exito = False
    for fn_extraer, fn_subir, clave in _HOJAS:
        try:
            recs = fn_extraer(ruta, fecha)
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
    try:
        fechas = resolver_fechas(args)
    except ValueError as exc:
        log.error(str(exc))
        sys.exit(1)

    log.info("Fechas a reprocesar: %s", [str(f) for f in fechas])

    errores = sum(1 for f in fechas if not procesar_fecha(f, args.solo_json))

    if errores:
        log.error("%d fecha(s) con error.", errores)
        sys.exit(1)
    log.info("=== Reprocesamiento completado ===")


if __name__ == "__main__":
    main()
