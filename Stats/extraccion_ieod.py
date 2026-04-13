"""
extraccion_ieod.py — Extrae datos de hojas del IEOD y genera JSON por fecha.

Puede ejecutarse directamente (asume que el Excel ya fue descargado):
    python extraccion_ieod.py --fecha 2026-04-09
    python extraccion_ieod.py --fecha-inicio 2026-04-01 --fecha-fin 2026-04-09
"""

import argparse
import json
import logging
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

from descarga_ieod import carpeta_anio, ruta_xlsx  # rutas compartidas

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------

def ruta_json(fecha: date) -> Path:
    return carpeta_anio(fecha) / f"{fecha.isoformat()}.json"


# ---------------------------------------------------------------------------
# Helpers internos de lectura y conversión
# ---------------------------------------------------------------------------

_DT_FMTS = [
    "%d/%m/%Y %H:%M:%S",   # EVENTOS: "07/04/2026 23:14:00"
    "%d/%m/%Y %H:%M",      # MANTENIMIENTO: "07/04/2026 00:00"
    "%Y-%m-%d %H:%M:%S",   # fallback ISO
    "%Y-%m-%d %H:%M",
]

_DATE_FMTS = [
    "%d/%m/%Y",            # RESTRIC_OPE FECHA: "07/04/2026"
    "%Y-%m-%d",
]


def _v(row: pd.Series, col: str) -> str:
    """Valor de una columna como string limpio; 'nan' si no existe o es vacío."""
    v = row.get(col, "")
    return str(v).strip() if v is not None else "nan"


def _str(row: pd.Series, col: str):
    v = _v(row, col)
    return None if v in ("nan", "") else v


def _float(row: pd.Series, col: str):
    v = _v(row, col)
    if v in ("nan", ""):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _dt(row: pd.Series, col: str):
    """Parsea datetime en formato DD/MM/YYYY HH:MM[:SS] → 'YYYY-MM-DD HH:MM:SS', o None."""
    v = _v(row, col)
    if v in ("nan", ""):
        return None
    for fmt in _DT_FMTS:
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
    log.debug("No se pudo parsear datetime: %r", v)
    return None


def _fecha_es(row: pd.Series, col: str):
    """Parsea fecha DD/MM/YYYY → 'YYYY-MM-DD', o None."""
    v = _v(row, col)
    if v in ("nan", ""):
        return None
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    log.debug("No se pudo parsear fecha: %r", v)
    return None


def _leer_hoja_simple(
    ruta: Path,
    sheet_name: str,
    col_clave: int = 0,
    fila_cabecera: int = 5,
) -> pd.DataFrame:
    """
    Layout estándar de las hojas IEOD (excepto DESPACHO_EJECUTADO):
      - iloc[fila_cabecera] (default iloc[5] = Excel fila 6): cabeceras de columna
      - iloc[fila_cabecera+1:]: datos
      - Columna 0: siempre vacía (merged cells de presentación) → se descarta

    Manejo de cantidad variable de filas:
      1. Normaliza headers: colapsa whitespace/newlines a un espacio
      2. dropna(how='all'): elimina filas completamente vacías al final
      3. dropna(subset=[col_clave]): elimina filas sin valor en la columna identificadora
    """
    df_raw = pd.read_excel(
        ruta, sheet_name=sheet_name, header=None, dtype=str, engine="openpyxl"
    )
    headers = (
        df_raw.iloc[fila_cabecera, 1:]
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
        .tolist()
    )
    df = df_raw.iloc[fila_cabecera + 1 :, 1:].copy()
    df.columns = headers
    df = df.dropna(how="all")
    df = df.dropna(subset=[headers[col_clave]])
    return df.reset_index(drop=True)


# Patrón para validar filas de hora (HH:MM) — descarta filas de totales (PROM, INICIO, FINAL)
_RE_HORA = re.compile(r"^\d{2}:\d{2}$")


def _leer_cabecera_multinivel(
    df_raw: pd.DataFrame,
    fila_inicio: int,
    claves: list[str],
    n_data_cols: int,
    unidades_fila: int | None = None,
) -> list[dict]:
    """
    Lee un bloque de filas-cabecera de hojas IEOD con múltiples niveles jerárquicos
    (PRINCIP_CAUDALES, PRINCIP_VOLÚMENES).

    - Columna 0 contiene etiquetas de fila (EMPRESA, CUENCA…) → se descarta.
    - Aplica ffill horizontal para rellenar celdas combinadas de empresa.
    - Retorna lista de dicts, uno por columna de datos.

    Args:
        fila_inicio:   iloc de la primera fila-cabecera (empresa)
        claves:        nombres para cada nivel [empresa, cuenca, instalacion, equipo, tipo]
        n_data_cols:   número de columnas de datos (sin col 0)
        unidades_fila: iloc de la fila de unidades (None si es uniforme)
    """
    n = len(claves)
    block = df_raw.iloc[fila_inicio : fila_inicio + n, 1 : 1 + n_data_cols].copy()
    block = block.ffill(axis=1)

    metas = []
    for col_idx in range(block.shape[1]):
        meta = {
            clave: str(block.iloc[nivel, col_idx]).strip()
            for nivel, clave in enumerate(claves)
        }
        if unidades_fila is not None:
            meta["unidad"] = str(df_raw.iloc[unidades_fila, col_idx + 1]).strip()
        metas.append(meta)
    return metas


# ---------------------------------------------------------------------------
# Extracción — DESPACHO_EJECUTADO
# ---------------------------------------------------------------------------

def extraer_despacho(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja DESPACHO_EJECUTADO y retorna registros en formato panel:
      [{"fecha": "YYYY-MM-DD", "hora": "HH:MM", "central": "...", "produccion": float}, ...]

    Estructura del Excel (diferente al resto de hojas):
      Fila 10 (iloc[9])  → nombres de centrales (desde col 2)
      Filas 11+ (iloc[10:]) → 48 períodos de 30 min (hora en col 1)
    """
    df_raw = pd.read_excel(
        ruta_archivo,
        sheet_name="DESPACHO_EJECUTADO",
        header=None,
        dtype=str,
        engine="openpyxl",
    )

    centrales = df_raw.iloc[9, 2:].tolist()
    datos = df_raw.iloc[10:, :]
    datos = datos[datos.iloc[:, 1].notna()]

    fecha_str = fecha.isoformat()
    records = []

    for _, row in datos.iterrows():
        hora = str(row.iloc[1]).strip()
        if not hora or hora == "nan":
            continue

        for i, central in enumerate(centrales):
            if pd.isna(central) or str(central).strip() in ("MW", "nan", ""):
                continue

            valor_raw = row.iloc[i + 2]
            if pd.isna(valor_raw) or str(valor_raw).strip() in ("nan", ""):
                continue

            try:
                produccion = float(valor_raw)
            except (ValueError, TypeError):
                continue

            records.append({
                "fecha": fecha_str,
                "hora": hora,
                "central": str(central).strip(),
                "produccion": produccion,
            })

    log.info("[%s] Registros extraídos de DESPACHO_EJECUTADO: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — EVENTOS
# ---------------------------------------------------------------------------

def extraer_eventos(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja EVENTOS. Filtra por columna clave: TIPO DE EVENTO.
    Fechas de inicio/final en formato DD/MM/YYYY HH:MM:SS.
    """
    df = _leer_hoja_simple(ruta_archivo, "EVENTOS", col_clave=0)
    fecha_str = fecha.isoformat()
    records = []
    for _, row in df.iterrows():
        tipo = _str(row, "TIPO DE EVENTO")
        if not tipo:
            continue
        inicio = _dt(row, "INICIO")
        if not inicio:
            continue  # INICIO es parte de la PK → obligatorio
        records.append({
            "fecha": fecha_str,
            "tipo_evento": tipo,
            "empresa": _str(row, "EMPRESA"),
            "ubicacion": _str(row, "UBICACIÓN"),
            "tipo_equipo": _str(row, "TIPO DE EQUIPO"),
            "equipo": _str(row, "EQUIPO"),
            "inicio": inicio,
            "final": _dt(row, "FINAL"),
            "descripcion": _str(row, "DESCRIPCIÓN"),
            "mw_indisp": _float(row, "MW INDISP."),
            "interrupcion": _str(row, "INTERRUPCIÓN (SI/NO)"),
            "tension_falla_kv": _float(row, "TENSIÓN DE FALLA (kV)"),
        })
    log.info("[%s] Registros extraídos de EVENTOS: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — RESTRIC_OPE
# ---------------------------------------------------------------------------

def extraer_restric_ope(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja RESTRIC_OPE. Filtra por columna clave: FECHA (col 0).
    La fecha se toma del campo FECHA de la hoja (no del nombre del archivo).
    Hora inicio/fin se guarda como string porque COES usa "24:00" (inválido para TIME).
    """
    df = _leer_hoja_simple(ruta_archivo, "RESTRIC_OPE", col_clave=0)
    records = []
    for _, row in df.iterrows():
        empresa = _str(row, "EMPRESA")
        if not empresa:
            continue
        records.append({
            "fecha": _fecha_es(row, "FECHA") or fecha.isoformat(),
            "hora_inicio": _str(row, "HORA INICIO"),
            "hora_fin": _str(row, "HORA FINAL"),
            "empresa": empresa,
            "ubicacion": _str(row, "UBICACIÓN"),
            "tipo_equipo": _str(row, "T.Eq."),
            "equipo": _str(row, "EQUIPO"),
            "descripcion": _str(row, "DESCRIPCIÓN"),
        })
    log.info("[%s] Registros extraídos de RESTRIC_OPE: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — MANTENIMIENTO EJECUTADOS
# ---------------------------------------------------------------------------

def extraer_mantenimiento(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja MANTENIMIENTO EJECUTADOS. Filtra por columna clave: Empresa (col 0).
    Fechas de inicio/final en formato DD/MM/YYYY HH:MM.
    """
    df = _leer_hoja_simple(ruta_archivo, "MANTENIMIENTO EJECUTADOS", col_clave=0)
    fecha_str = fecha.isoformat()
    records = []
    for _, row in df.iterrows():
        empresa = _str(row, "Empresa")
        if not empresa:
            continue
        inicio = _dt(row, "Inicio")
        if not inicio:
            continue  # Inicio es parte de la PK → obligatorio
        cod_raw = _float(row, "CodEq")
        records.append({
            "fecha": fecha_str,
            "empresa": empresa,
            "ubicacion": _str(row, "Ubicación"),
            "equipo": _str(row, "Equipo"),
            "inicio": inicio,
            "final": _dt(row, "Final"),
            "descripcion": _str(row, "Descripción"),
            "mw_indisp": _float(row, "MW Indisp."),
            "programado": _str(row, "Prog."),
            "disponibilidad": _str(row, "Dispon"),
            "interrupcion": _str(row, "Interrupc."),
            "tipo": _str(row, "Tipo"),
            "cod_eq": int(cod_raw) if cod_raw is not None else None,
            "tipo_eq_osinerg": _str(row, "TipoEq_Osinerg"),
        })
    log.info("[%s] Registros extraídos de MANTENIMIENTO EJECUTADOS: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — DEMANDA_AREAS
# ---------------------------------------------------------------------------

def extraer_demanda_areas(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja DEMANDA_AREAS y retorna registros en formato panel:
      [{"fecha": "YYYY-MM-DD", "hora": "HH:MM", "area": "...", "demanda_mw": float}, ...]

    Layout: col 0 = NaN, col 1 = hora, cols 2-7 = áreas (MW).
    Cabeceras de área en iloc[6], datos en iloc[7:].
    """
    df_raw = pd.read_excel(
        ruta_archivo, sheet_name="DEMANDA_AREAS", header=None, dtype=str, engine="openpyxl"
    )
    areas = [str(v).strip() for v in df_raw.iloc[6, 2:].tolist()]

    fecha_str = fecha.isoformat()
    records = []
    for _, row in df_raw.iloc[7:].iterrows():
        hora = str(row.iloc[1]).strip()
        if not _RE_HORA.match(hora):
            continue
        for i, area in enumerate(areas):
            if not area or area == "nan":
                continue
            val_raw = str(row.iloc[i + 2]).strip()
            if val_raw in ("nan", "", "--"):
                continue
            try:
                records.append({"fecha": fecha_str, "hora": hora, "area": area, "demanda_mw": float(val_raw)})
            except (ValueError, TypeError):
                continue

    log.info("[%s] Registros extraídos de DEMANDA_AREAS: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — PRINCIP_CAUDALES
# ---------------------------------------------------------------------------

def extraer_princip_caudales(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja PRINCIP_CAUDALES (caudales de centrales hidroeléctricas en m³/s).

    Cabecera de 5 niveles en iloc[5:10] (empresa, cuenca, instalacion, equipo, tipo_caudal).
    Datos en iloc[11:], col 0 = hora. Filtra filas de totales (PROM, INICIO, FINAL).
    Valores faltantes representados como '--'.
    """
    df_raw = pd.read_excel(
        ruta_archivo, sheet_name="PRINCIP_CAUDALES", header=None, dtype=str, engine="openpyxl"
    )
    n_data_cols = df_raw.shape[1] - 1
    metas = _leer_cabecera_multinivel(
        df_raw, fila_inicio=5,
        claves=["empresa", "cuenca", "instalacion", "equipo", "tipo_caudal"],
        n_data_cols=n_data_cols,
    )

    fecha_str = fecha.isoformat()
    records = []
    for _, row in df_raw.iloc[11:].iterrows():
        hora = str(row.iloc[0]).strip()
        if not _RE_HORA.match(hora):
            continue
        for i, meta in enumerate(metas):
            val_raw = str(row.iloc[i + 1]).strip()
            if val_raw in ("nan", "", "--"):
                continue
            try:
                records.append({"fecha": fecha_str, "hora": hora, **meta, "caudal_m3s": float(val_raw)})
            except (ValueError, TypeError):
                continue

    log.info("[%s] Registros extraídos de PRINCIP_CAUDALES: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — PRINCIP_VOLÚMENES
# ---------------------------------------------------------------------------

def extraer_princip_volumenes(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja PRINCIP_VOLÚMENES (niveles en msnm y volúmenes útiles en Hm³).

    Cabecera de 5 niveles en iloc[4:9] (empresa, cuenca, instalacion, equipo, tipo_medicion).
    Fila de unidades en iloc[9] (varía por columna: msnm o Hm3).
    Datos en iloc[10:], col 0 = hora.
    Valores faltantes representados como '--'.
    """
    df_raw = pd.read_excel(
        ruta_archivo, sheet_name="PRINCIP_VOL\xdaMENES", header=None, dtype=str, engine="openpyxl"
    )
    n_data_cols = df_raw.shape[1] - 1
    metas = _leer_cabecera_multinivel(
        df_raw, fila_inicio=4,
        claves=["empresa", "cuenca", "instalacion", "equipo", "tipo_medicion"],
        n_data_cols=n_data_cols,
        unidades_fila=9,
    )

    fecha_str = fecha.isoformat()
    records = []
    for _, row in df_raw.iloc[10:].iterrows():
        hora = str(row.iloc[0]).strip()
        if not _RE_HORA.match(hora):
            continue
        for i, meta in enumerate(metas):
            val_raw = str(row.iloc[i + 1]).strip()
            if val_raw in ("nan", "", "--"):
                continue
            try:
                records.append({"fecha": fecha_str, "hora": hora, **meta, "valor": float(val_raw)})
            except (ValueError, TypeError):
                continue

    log.info("[%s] Registros extraídos de PRINCIP_VOLÚMENES: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — CONSUMO_COMB
# ---------------------------------------------------------------------------

def extraer_consumo_comb(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja CONSUMO_COMB (consumo de combustible por central).

    La columna de consumo tiene nombre dinámico (e.g. '06/Apr') → se accede
    por posición (última columna del DataFrame).
    """
    df = _leer_hoja_simple(ruta_archivo, "CONSUMO_COMB", col_clave=0, fila_cabecera=6)
    fecha_str = fecha.isoformat()
    records = []
    for _, row in df.iterrows():
        empresa = _str(row, df.columns[0])  # EMPRESA
        if not empresa:
            continue
        val_raw = str(row.iloc[5]).strip()  # columna de consumo (posición fija)
        consumo = None
        if val_raw not in ("nan", "", "--"):
            try:
                consumo = float(val_raw)
            except (ValueError, TypeError):
                pass
        records.append({
            "fecha": fecha_str,
            "empresa": empresa,
            "central": _str(row, df.columns[1]),
            "medidor": _str(row, df.columns[2]),
            "tipo_combustible": _str(row, df.columns[3]),
            "unidad": _str(row, df.columns[4]),
            "consumo": consumo,
        })
    log.info("[%s] Registros extraídos de CONSUMO_COMB: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — DISPONIBILIDAD_GAS
# ---------------------------------------------------------------------------

def extraer_disponibilidad_gas(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja DISPONIBILIDAD_GAS (disponibilidad de gas por gasoducto).
    ~9 registros/día. INICIAL/FINAL en formato DD/MM/YYYY HH:MM.
    """
    df = _leer_hoja_simple(ruta_archivo, "DISPONIBILIDAD_GAS", col_clave=1, fila_cabecera=6)
    fecha_str = fecha.isoformat()
    records = []
    for _, row in df.iterrows():
        empresa = _str(row, "EMPRESA")
        if not empresa:
            continue
        vol_raw = str(row.iloc[3]).strip()  # VOLUMEN DE GAS (Mm3) — nombre con paréntesis
        volumen = None
        if vol_raw not in ("nan", "", "--"):
            try:
                volumen = float(vol_raw)
            except (ValueError, TypeError):
                pass
        records.append({
            "fecha": fecha_str,
            "empresa": empresa,
            "gaseoducto": _str(row, "GASEODUCTO"),
            "volumen_mm3": volumen,
            "inicio": _dt(row, "INICIAL"),
            "final": _dt(row, "FINAL"),
            "observaciones": _str(row, "OBSERVACIONES"),
        })
    log.info("[%s] Registros extraídos de DISPONIBILIDAD_GAS: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Extracción — INTERCONEXIONES
# ---------------------------------------------------------------------------

def extraer_interconexiones(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja INTERCONEXIONES (flujo MW en líneas de transmisión entre regiones).

    Cabecera de 3 niveles:
      iloc[4] = código numérico (NaN en columnas TOTAL)
      iloc[5] = grupo/SS.EE. (celdas combinadas → ffill)
      iloc[6] = nombre de línea (contiene \\n → normalizar a espacio)
    Datos: iloc[7:], col 0 = fecha string, col 1 = hora HH:MM.
    Columnas TOTAL (sin código) se omiten.
    Filas de footer (MÁXIMO, MÍNIMO, PROMEDIO, EMPRESA) se filtran con _RE_HORA.
    """
    df_raw = pd.read_excel(
        ruta_archivo, sheet_name="INTERCONEXIONES", header=None, dtype=str, engine="openpyxl"
    )

    codigos = df_raw.iloc[4, :].tolist()
    grupos_raw = pd.Series(df_raw.iloc[5, :].tolist())
    grupos_raw = grupos_raw.where(~grupos_raw.isin(["nan", "None", ""]), other=None)
    grupos_filled = grupos_raw.ffill().tolist()
    lineas = df_raw.iloc[6, :].tolist()

    # Recopilar columnas de datos válidas (tienen código numérico)
    data_cols = []
    for col_idx in range(2, len(codigos)):
        cod_str = str(codigos[col_idx]).strip()
        if cod_str in ("nan", "", "None"):
            continue  # columna TOTAL o vacía
        try:
            codigo = str(int(float(cod_str)))
        except (ValueError, TypeError):
            continue
        linea = str(lineas[col_idx]).strip().replace("\n", " ")
        grupo = str(grupos_filled[col_idx]).strip() if grupos_filled[col_idx] else None
        data_cols.append((col_idx, codigo, linea, grupo))

    fecha_str = fecha.isoformat()
    records = []
    for _, row in df_raw.iloc[7:].iterrows():
        hora = str(row.iloc[1]).strip()
        if not _RE_HORA.match(hora):
            continue
        for col_idx, codigo, linea, grupo in data_cols:
            val_raw = str(row.iloc[col_idx]).strip()
            if val_raw in ("nan", "", "--"):
                continue
            try:
                records.append({
                    "fecha": fecha_str,
                    "hora": hora,
                    "codigo": codigo,
                    "linea": linea,
                    "grupo": grupo,
                    "flujo_mw": float(val_raw),
                })
            except (ValueError, TypeError):
                continue

    log.info("[%s] Registros extraídos de INTERCONEXIONES: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# Helper — parseo de fecha en COSTO_OPE_EJEC
# ---------------------------------------------------------------------------

def _parsear_fecha_costo(cell: str, fecha: date):
    """
    Convierte strings tipo 'LUN 30', 'MAR 31', 'MIE 1' al date correcto.
    Extrae el número de día y lo ubica en el mes de `fecha` o en el mes anterior
    si el número supera el día del archivo (indicando que cruza fin de mes).
    """
    m = re.search(r"\d+", str(cell))
    if not m:
        return None
    day = int(m.group())
    if day <= fecha.day:
        try:
            return date(fecha.year, fecha.month, day)
        except ValueError:
            return None
    else:
        # día pertenece al mes anterior
        first_of_month = date(fecha.year, fecha.month, 1)
        prev_month_last = first_of_month - timedelta(days=1)
        try:
            return date(prev_month_last.year, prev_month_last.month, day)
        except ValueError:
            return None


# ---------------------------------------------------------------------------
# Extracción — COSTO_OPE_EJEC
# ---------------------------------------------------------------------------

def extraer_costo_ope_ejec(ruta_archivo: Path, fecha: date) -> list[dict]:
    """
    Lee la hoja COSTO_OPE_EJEC (costos de operación ejecutado vs. programado).
    Contiene ~8 días rodantes finalizando en la fecha del archivo.
    La columna Fecha tiene strings como 'LUN 30', 'MAR 31' → _parsear_fecha_costo().
    """
    df = _leer_hoja_simple(ruta_archivo, "COSTO_OPE_EJEC", col_clave=0, fila_cabecera=6)
    records = []
    for _, row in df.iterrows():
        fecha_rec = _parsear_fecha_costo(str(row.iloc[0]).strip(), fecha)
        if fecha_rec is None:
            continue
        ejec_raw = str(row.iloc[1]).strip()
        prog_raw = str(row.iloc[2]).strip()
        pct_raw = str(row.iloc[3]).strip()
        records.append({
            "fecha": fecha_rec.isoformat(),
            "costo_ejecutado": float(ejec_raw) if ejec_raw not in ("nan", "", "--") else None,
            "costo_programado": float(prog_raw) if prog_raw not in ("nan", "", "--") else None,
            "porcentaje": float(pct_raw) if pct_raw not in ("nan", "", "--") else None,
        })
    log.info("[%s] Registros extraídos de COSTO_OPE_EJEC: %d", fecha, len(records))
    return records


# ---------------------------------------------------------------------------
# JSON por fecha
# ---------------------------------------------------------------------------

def guardar_en_json(registros: list[dict], fecha: date, clave: str) -> Path:
    """
    Guarda registros en {año}/{fecha}.json bajo la clave indicada.
    Si el archivo ya existe, actualiza solo esa clave sin borrar otras.
    """
    destino = ruta_json(fecha)

    if destino.exists():
        with open(destino, "r", encoding="utf-8") as f:
            contenido = json.load(f)
    else:
        contenido = {}

    contenido[clave] = registros

    with open(destino, "w", encoding="utf-8") as f:
        json.dump(contenido, f, ensure_ascii=False, separators=(",", ":"))

    log.info("JSON [%s] guardado: %s (%.1f KB)", clave, destino, destino.stat().st_size / 1024)
    return destino


def guardar_json(registros: list[dict], fecha: date) -> Path:
    """Wrapper de compatibilidad → guarda bajo la clave 'despacho_ejecutado'."""
    return guardar_en_json(registros, fecha, "despacho_ejecutado")


# ---------------------------------------------------------------------------
# CLI directo
# ---------------------------------------------------------------------------

_HOJAS_CLI = [
    (extraer_despacho,           "despacho_ejecutado"),
    (extraer_eventos,            "eventos"),
    (extraer_restric_ope,        "restric_ope"),
    (extraer_mantenimiento,      "mantenimiento_ejecutados"),
    (extraer_demanda_areas,      "demanda_areas"),
    (extraer_princip_caudales,   "princip_caudales"),
    (extraer_princip_volumenes,  "princip_volumenes"),
    (extraer_consumo_comb,       "consumo_comb"),
    (extraer_disponibilidad_gas, "disponibilidad_gas"),
    (extraer_interconexiones,    "interconexiones"),
    (extraer_costo_ope_ejec,     "costo_ope_ejec"),
]


def _parse_args():
    p = argparse.ArgumentParser(
        description="Extrae datos IEOD desde Excel y genera JSON. El Excel debe existir."
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
        ruta = ruta_xlsx(_fecha)
        if not ruta.exists():
            log.error("[%s] Excel no encontrado: %s", _fecha, ruta)
            _errores += 1
            continue
        try:
            for fn, clave in _HOJAS_CLI:
                recs = fn(ruta, _fecha)
                if recs:
                    guardar_en_json(recs, _fecha, clave)
                else:
                    log.warning("[%s] Sin registros en %s.", _fecha, clave)
        except Exception as exc:
            log.error("[%s] Error: %s", _fecha, exc)
            _errores += 1
    sys.exit(1 if _errores else 0)
