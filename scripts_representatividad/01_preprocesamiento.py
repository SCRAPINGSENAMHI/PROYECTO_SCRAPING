"""
Script 01 — Preprocesamiento
==============================
Metodología: OMM-Nº 8 (2024) · OMM-Nº 168 (2008) · SENAMHI (2020)
PDF §1–3

Acciones:
  1. Fusiona stations_hist_portal.csv + Maestra_de_estaciones_Senamhi.xlsx
  2. Deduplica por código
  3. Spatial join con SECTORES.shp → campo 'sector'
  4. Extrae altitud DEM → calcula delta_z y clase_diagnostico (§3)
  5. Asigna parámetros de representatividad según sector (§2.2)
  6. Exporta DATA/representatividad_v2/estaciones_prep.csv

Salida mínima requerida por scripts siguientes:
  cod_qc, nombre, lat, lon, altitud_oficial, dept,
  sector, altitud_dem, delta_z, clase_diagnostico,
  umbral_pp, umbral_t, radio_pp, radio_t
"""

import pathlib, sys, warnings
import numpy as np
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.transform import rowcol
warnings.filterwarnings('ignore')

# ── Rutas ─────────────────────────────────────────────────────────────────────
ROOT   = pathlib.Path(__file__).resolve().parent.parent
DATA   = ROOT / 'DATA'
OUT    = DATA / 'representatividad_v2'
OUT.mkdir(exist_ok=True)

CSV_HIST   = DATA / 'stations_hist_portal.csv'
XLSX_MAEST = DATA / 'Maestra_de_estaciones_Senamhi.xlsx'
SHP_SECT   = DATA / 'SECTOR_CLIMATICO' / 'SECTORES.shp'
DEM_PATH   = DATA / 'DEM' / 'DEM_FINAL.tif'

# ── Parámetros por sector (§2.2 PDF) ──────────────────────────────────────────
# Mapeamos los 15 sectores del shapefile a grupos de parámetros
# Regla: para transiciones se usa el más restrictivo (radio menor)
PARAMS = {
    'COSTA NORTE':              {'umbral_pp': 80,  'umbral_t': 150, 'radio_pp': 70,  'radio_t': 120},
    'COSTA CENTRO':             {'umbral_pp': 80,  'umbral_t': 150, 'radio_pp': 60,  'radio_t': 100},
    'COSTA SUR':                {'umbral_pp': 80,  'umbral_t': 150, 'radio_pp': 70,  'radio_t': 120},
    'SIERRA NORTE OCCIDENTAL':  {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 15,  'radio_t': 30 },
    'SIERRA NORTE ORIENTAL':    {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 12,  'radio_t': 25 },
    'SIERRA CENTRAL OCCIDENTAL':{'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 12,  'radio_t': 25 },
    'SIERRA CENTRAL ORIENTAL':  {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 10,  'radio_t': 20 },
    'SIERRA SUR OCCIDENTAL':    {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 30,  'radio_t': 60 },
    'SIERRA SUR ORIENTAL':      {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 20,  'radio_t': 40 },
    'SELVA NORTE ALTA':         {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 35,  'radio_t': 70 },
    'SELVA NORTE BAJA':         {'umbral_pp': 80,  'umbral_t': 150, 'radio_pp': 60,  'radio_t': 100},
    'SELVA CENTRAL ALTA':       {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 30,  'radio_t': 60 },
    'SELVA CENTRAL BAJA':       {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 40,  'radio_t': 80 },
    'SELVA SUR ALTA':           {'umbral_pp': 100, 'umbral_t': 200, 'radio_pp': 35,  'radio_t': 70 },
    'SELVA SUR BAJA':           {'umbral_pp': 80,  'umbral_t': 150, 'radio_pp': 60,  'radio_t': 100},
}
# Fallback: Sierra Central Occidental (más restrictivo genérico)
PARAMS_DEFAULT = PARAMS['SIERRA CENTRAL OCCIDENTAL']

# ── Diagnóstico altimétrico (§3.2 PDF) ────────────────────────────────────────
def clasificar_delta_z(dz):
    adz = abs(dz)
    if adz < 50:   return 'Consistente'
    if adz < 150:  return 'Discrepancia leve'
    if adz < 300:  return 'Discrepancia moderada'
    return 'Sospechosa'

# ── 1. Cargar estaciones históricas ───────────────────────────────────────────
print('[01] Cargando stations_hist_portal.csv ...')
df_hist = pd.read_csv(CSV_HIST)
df_hist = df_hist.rename(columns={
    'estacion': 'nombre', 'cod_qc': 'cod_qc',
    'lat': 'lat', 'lon': 'lon',
    'altitud': 'altitud_oficial',
    'departamento': 'dept'
})
df_hist['fuente'] = 'historica'
df_hist['cod_qc'] = df_hist['cod_qc'].astype(str).str.strip()
# Normalizar cod numérico para deduplicación posterior
df_hist['cod_num'] = df_hist['cod_qc'].str.replace('qc', '', regex=False).str.lstrip('0')
print(f'    → {len(df_hist)} estaciones históricas')

# ── 2. Cargar Maestra ─────────────────────────────────────────────────────────
print('[01] Cargando Maestra_de_estaciones_Senamhi.xlsx ...')
df_maest = pd.read_excel(XLSX_MAEST)
df_maest = df_maest.rename(columns={
    'NOMBRE_ESTACION': 'nombre',
    'CODIGO':          'cod',
    'LATITUD':         'lat',
    'LONGITUD':        'lon',
    'ALTITUD':         'altitud_oficial',
    'DEPARTAMENTO':    'dept'
})
# Construir cod_qc estilo qcXXXXXXXX
df_maest['cod_qc']  = 'qc' + df_maest['cod'].astype(str).str.zfill(8)
df_maest['cod_num'] = df_maest['cod'].astype(str).str.lstrip('0')
df_maest['fuente']  = 'maestra'
print(f'    → {len(df_maest)} estaciones maestra')

# ── 3. Fusión y deduplicación ─────────────────────────────────────────────────
# Columnas comunes
COLS = ['cod_qc', 'cod_num', 'nombre', 'lat', 'lon', 'altitud_oficial', 'dept', 'fuente']
df_h = df_hist[[c for c in COLS if c in df_hist.columns]].copy()
df_m = df_maest[[c for c in COLS if c in df_maest.columns]].copy()

df_all = pd.concat([df_h, df_m], ignore_index=True)

# Eliminar filas sin coordenadas válidas
df_all = df_all.dropna(subset=['lat', 'lon'])
df_all = df_all[df_all['lat'].between(-20, 0) & df_all['lon'].between(-82, -68)]

# Deduplicar: si el mismo cod_qc aparece en ambas fuentes, conservar la
# versión 'historica' (tiene altitud de campo verificada)
df_all = df_all.sort_values('fuente').drop_duplicates(subset='cod_qc', keep='first')
df_all = df_all.reset_index(drop=True)
print(f'[01] Estaciones fusionadas y deduplicadas: {len(df_all)}')

# ── 4. Spatial join con SECTORES.shp ─────────────────────────────────────────
print('[01] Asignando sector climático (spatial join) ...')
gdf_sect = gpd.read_file(SHP_SECT)  # EPSG:4326

gdf_est = gpd.GeoDataFrame(
    df_all,
    geometry=gpd.points_from_xy(df_all['lon'], df_all['lat']),
    crs='EPSG:4326'
)

gdf_joined = gpd.sjoin(
    gdf_est,
    gdf_sect[['SECTOR', 'COD_SECTOR', 'geometry']],
    how='left',
    predicate='within'
)
# Puede haber duplicados si un punto cae en el borde de dos polígonos
gdf_joined = gdf_joined[~gdf_joined.index.duplicated(keep='first')]

df_all['sector']     = gdf_joined['SECTOR'].values
df_all['cod_sector'] = gdf_joined['COD_SECTOR'].values

sin_sector = df_all['sector'].isna().sum()
print(f'    → Estaciones sin sector asignado: {sin_sector}')
if sin_sector > 0:
    # Asignar el sector más cercano a las que quedaron sin match (bordes costeros)
    from shapely.geometry import Point
    gdf_sect_proj = gdf_sect.copy()
    for idx, row in df_all[df_all['sector'].isna()].iterrows():
        pt = Point(row['lon'], row['lat'])
        distances = gdf_sect_proj.geometry.distance(pt)
        nearest   = gdf_sect_proj.iloc[distances.idxmin()]
        df_all.at[idx, 'sector']     = nearest['SECTOR']
        df_all.at[idx, 'cod_sector'] = nearest['COD_SECTOR']
    print(f'    → Asignados por cercanía: {sin_sector}')

# ── 5. Diagnóstico altimétrico DEM (§3.2) ────────────────────────────────────
print('[01] Extrayendo altitud DEM y calculando delta_z ...')
altitudes_dem = np.full(len(df_all), np.nan)

with rasterio.open(DEM_PATH) as src:
    nodata = src.nodata
    # Reproyectar coordenadas WGS84 → UTM 32719 para muestreo
    from pyproj import Transformer
    transformer = Transformer.from_crs('EPSG:4326', str(src.crs), always_xy=True)

    coords_utm = [
        transformer.transform(row['lon'], row['lat'])
        for _, row in df_all.iterrows()
    ]

    # Muestreo bilineal: rasterio.sample devuelve valores del píxel
    for i, (x_utm, y_utm) in enumerate(coords_utm):
        try:
            val = list(src.sample([(x_utm, y_utm)]))[0][0]
            if val != nodata and not np.isnan(val) and abs(val) < 9000:
                altitudes_dem[i] = float(val)
        except Exception:
            pass

df_all['altitud_dem']    = altitudes_dem
df_all['altitud_oficial'] = pd.to_numeric(df_all['altitud_oficial'], errors='coerce')
df_all['delta_z']        = df_all['altitud_oficial'] - df_all['altitud_dem']
df_all['clase_diagnostico'] = df_all['delta_z'].apply(
    lambda dz: clasificar_delta_z(dz) if pd.notna(dz) else 'Sin altitud oficial'
)

print(f'    → DEM extraído para {(~np.isnan(altitudes_dem)).sum()} / {len(df_all)} estaciones')
print('    → Distribución diagnóstico:')
print(df_all['clase_diagnostico'].value_counts().to_string())

# ── 6. Asignar parámetros de representatividad ────────────────────────────────
print('[01] Asignando parámetros por sector ...')

def get_params(sector_name):
    if pd.isna(sector_name):
        return PARAMS_DEFAULT
    key = str(sector_name).strip().upper()
    return PARAMS.get(key, PARAMS_DEFAULT)

for param in ['umbral_pp', 'umbral_t', 'radio_pp', 'radio_t']:
    df_all[param] = df_all['sector'].apply(lambda s: get_params(s)[param])

# Resumen parámetros
print('    → Parámetros asignados:')
print(df_all.groupby('sector')[['radio_pp', 'radio_t']].first().to_string())

# ── 7. Exportar ───────────────────────────────────────────────────────────────
COLS_OUT = [
    'cod_qc', 'nombre', 'lat', 'lon',
    'altitud_oficial', 'altitud_dem', 'delta_z', 'clase_diagnostico',
    'dept', 'fuente', 'sector', 'cod_sector',
    'umbral_pp', 'umbral_t', 'radio_pp', 'radio_t'
]
df_out = df_all[[c for c in COLS_OUT if c in df_all.columns]].copy()
out_path = OUT / 'estaciones_prep.csv'
df_out.to_csv(out_path, index=False, encoding='utf-8')
print(f'\n[01] ✓ Guardado: {out_path}')
print(f'     Total estaciones: {len(df_out)}')
print(f'     Sectores únicos:  {df_out["sector"].nunique()}')
print(f'     Con coord+sector: {df_out["sector"].notna().sum()}')
