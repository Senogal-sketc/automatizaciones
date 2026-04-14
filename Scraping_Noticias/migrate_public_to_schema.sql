-- migrate_public_to_schema.sql
-- Migra los datos de public.normas_noticias → normas_noticias.normas_noticias
--
-- Instrucciones:
--   1. Abre Supabase Dashboard → SQL Editor
--   2. Pega este script completo y ejecútalo
--   3. Revisa el conteo al final para verificar
--   4. Cuando estés seguro, descomenta el bloque DROP al final
--
-- Es seguro re-ejecutar: ON CONFLICT DO NOTHING evita duplicados.

-- ── Paso 1: crear esquema y tabla destino (idempotente) ──────────────────────
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

-- ── Paso 2: copiar datos (omite duplicados por link) ─────────────────────────
INSERT INTO normas_noticias.normas_noticias
    (fuente, link, res, titular, fecha_pub, sumilla,
     contenido, relevante, impacto, resumen, fecha_scraping, created_at)
SELECT
    fuente, link, res, titular, fecha_pub, sumilla,
    contenido, relevante, impacto, resumen, fecha_scraping, created_at
FROM public.normas_noticias
ON CONFLICT (link) DO NOTHING;

-- ── Paso 3: verificar conteos ─────────────────────────────────────────────────
SELECT
    'public.normas_noticias'             AS tabla,
    COUNT(*)                             AS total
FROM public.normas_noticias
UNION ALL
SELECT
    'normas_noticias.normas_noticias'    AS tabla,
    COUNT(*)                             AS total
FROM normas_noticias.normas_noticias;

-- ── Paso 4 (OPCIONAL): eliminar tabla del esquema public ─────────────────────
-- Solo descomenta cuando hayas verificado que los conteos coinciden
-- y el pipeline nuevo esté funcionando correctamente.
--
-- DROP TABLE public.normas_noticias;
