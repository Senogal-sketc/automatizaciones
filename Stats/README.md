# Stads — Pipeline IEOD → PostgreSQL

Pipeline modular para descargar el IEOD (Informe de la Operación Diaria) de COES,
extraer datos por hoja y subirlos a PostgreSQL.

---

## Estructura de archivos

```
Stads/
├── config.py              ← credenciales reales (NO va al repo, en .gitignore)
├── config.example.py      ← plantilla sin datos reales (sí va al repo)
├── requirements.txt       ← dependencias Python
│
├── descarga_ieod.py       ← módulo: descarga Excel de COES
├── extraccion_ieod.py     ← módulo: extrae hojas y genera JSON
├── subir_postgres.py      ← módulo: carga JSON a PostgreSQL
│
├── pipeline.py            ← orquestador principal (descarga + extrae + sube)
├── reprocesar.py          ← orquestador sin descarga (extrae + sube desde Excel existente)
│
└── README.md
```

Datos generados en `IEOD_BASE_DIR` (definido en `config.py`):
```
14 - IEOD/
├── 2026/
│   ├── 2026-04-07.xlsx      ← descargado de COES
│   ├── 2026-04-07.json      ← generado por el pipeline
│   └── ...
└── 2027/
    └── ...
```

---

## Configuración inicial

### 1. Copiar y completar credenciales

```bash
cp config.example.py config.py
```

Editar `config.py` con los valores reales:

| Variable | Descripción |
|---|---|
| `IEOD_BASE_DIR` | Ruta raíz donde se guardan los archivos por año |
| `PG_HOST` | Host de PostgreSQL |
| `PG_PORT` | Puerto (normalmente `5432`) |
| `PG_DB` | Nombre de la base de datos |
| `PG_USER` | Usuario |
| `PG_PASSWORD` | Contraseña |

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

---

## Uso

### `pipeline.py` — Orquestador principal

Descarga el Excel, extrae los datos y los sube a PostgreSQL.
**Por defecto procesa ayer.** Es lo que ejecuta GitHub Actions cada día.

```bash
python pipeline.py                                             # ayer
python pipeline.py --fecha 2026-04-09                         # fecha específica
python pipeline.py --fecha-inicio 2026-04-01 --fecha-fin 2026-04-09  # rango
python pipeline.py --fecha 2026-04-09 --solo-json             # sin subir a PostgreSQL
```

### `reprocesar.py` — Orquestador sin descarga

Re-extrae y sube datos desde Excels que ya fueron descargados.
Útil para reprocesar fechas históricas sin volver a descargar.
**Requiere especificar fecha explícitamente.**

```bash
python reprocesar.py --fecha 2026-04-09
python reprocesar.py --fecha-inicio 2026-04-01 --fecha-fin 2026-04-09
python reprocesar.py --fecha 2026-04-09 --solo-json
```

### Módulos individuales

Cada módulo también puede ejecutarse directamente (default: ayer):

```bash
python descarga_ieod.py --fecha 2026-04-09       # solo descargar
python extraccion_ieod.py --fecha 2026-04-09     # solo extraer (Excel debe existir)
python subir_postgres.py --fecha 2026-04-09      # solo subir (JSON debe existir)
```

---

## URL de descarga de COES

La URL se construye automáticamente en `descarga_ieod.py`:

```
https://www.coes.org.pe/portal/browser/download
  ?url=Post Operación/Reportes/IEOD/{año}/{MM}_{Mes}/{DD}/AnexoA_{DD}{MM}.xlsx
```

Ejemplo para el 9 de abril de 2026:
```
.../IEOD/2026/04_Abril/09/AnexoA_0904.xlsx
```

Si COES cambia la estructura de la URL, editar `construir_url_coes()` en `descarga_ieod.py`.

---

## Formato del JSON generado

Cada `{fecha}.json` usa una clave por hoja, lo que permite agregar nuevas hojas
al mismo archivo sin romper la estructura:

```json
{
  "despacho_ejecutado": [
    {"fecha": "2026-04-07", "hora": "00:30", "central": "SAN GABAN III", "produccion": 199.8},
    {"fecha": "2026-04-07", "hora": "00:30", "central": "MANTARO", "produccion": 648.6}
  ],
  "consumo_comb": [
    ...
  ]
}
```

Volumen aproximado: ~6 300 registros por día y hoja (48 períodos × ~130 centrales activas).

---

## Tabla en PostgreSQL

La tabla se crea automáticamente en el primer run:

```sql
CREATE TABLE IF NOT EXISTS despacho_ejecutado (
    fecha       DATE             NOT NULL,
    hora        TIME             NOT NULL,
    central     VARCHAR(200)     NOT NULL,
    produccion  DOUBLE PRECISION,
    PRIMARY KEY (fecha, hora, central)
);
```

La clave primaria `(fecha, hora, central)` garantiza que re-ejecutar el mismo día
no genera duplicados (`ON CONFLICT DO NOTHING`).

---

## GitHub Actions

El workflow `.github/workflows/ieod_daily.yml` corre todos los días a las
**12:00 PM Lima (UTC-5 = 17:00 UTC)**, ejecutando `pipeline.py` (procesa ayer por defecto).

### Secrets requeridos

Settings → Secrets and variables → Actions → New repository secret:

| Secret | Valor |
|---|---|
| `PG_HOST` | Host PostgreSQL |
| `PG_PORT` | Puerto |
| `PG_DB` | Base de datos |
| `PG_USER` | Usuario |
| `PG_PASSWORD` | Contraseña |
| `IEOD_BASE_DIR` | Ruta raíz en el runner, ej: `/tmp/ieod` |

### Disparar manualmente con fecha específica

GitHub → pestaña **Actions** → `IEOD – Pipeline diario` → **Run workflow**
→ ingresar fecha en formato `YYYY-MM-DD`.

---

## Agregar nuevas hojas del IEOD

Para extraer otra hoja (ej. `CONSUMO_COMB`), añadir una función a `extraccion_ieod.py`
siguiendo el mismo patrón que `extraer_despacho()`, y llamarla desde `pipeline.py`.

El resultado se agrega al mismo JSON bajo su propia clave:

```python
contenido["consumo_comb"] = registros_consumo
```

Cada script nuevo solo necesita `import config` para acceder a todas las credenciales.
