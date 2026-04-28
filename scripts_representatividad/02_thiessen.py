"""
Script 02 — Polígonos de Thiessen (Voronoi)
=============================================
Metodología: OMM-Nº 168 (2008) §2.4.2 · PDF §4.2

Acciones:
  1. Carga estaciones_prep.csv
  2. Reproyecta a EPSG:32719 (UTM zona 19S — igual que el DEM)
  3. Calcula diagrama de Voronoi sobre coordenadas UTM
  4. Recorta cada polígono al límite del Perú (shapefile departamentos)
  5. Exporta thiessen.gpkg con polígono por estación
  6. Guarda versión WGS84 para referencia visual

Nota: Se usa scipy.spatial.Voronoi + geometría shapely para construir
los polígonos finitos. El límite exterior se toma de la unión de
departamentos, que equivale al contorno nacional.
"""

import pathlib, warnings
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import MultiPoint, Polygon, MultiPolygon, box
from shapely.ops import unary_union
from scipy.spatial import Voronoi
warnings.filterwarnings('ignore')

# ── Rutas ─────────────────────────────────────────────────────────────────────
ROOT      = pathlib.Path(__file__).resolve().parent.parent
DATA      = ROOT / 'DATA'
OUT       = DATA / 'representatividad_v2'
SHP_DEPT  = DATA / 'DEPARTAMENTOS' / 'INEI_LIMITE_DEPARTAMENTAL_GEOGPSPERU_JUANSUYO_931381206.shp'
PREP_CSV  = OUT / 'estaciones_prep.csv'

# ── 1. Cargar estaciones ──────────────────────────────────────────────────────
print('[02] Cargando estaciones_prep.csv ...')
df = pd.read_csv(PREP_CSV)
df = df.dropna(subset=['lat', 'lon']).reset_index(drop=True)
print(f'     {len(df)} estaciones')

# ── 2. Construir GeoDataFrame y reproyectar a EPSG:32719 ──────────────────────
gdf = gpd.GeoDataFrame(
    df, geometry=gpd.points_from_xy(df['lon'], df['lat']), crs='EPSG:4326'
)
gdf_utm = gdf.to_crs('EPSG:32719')
print('[02] Reproyectado a EPSG:32719 (UTM zona 19S)')

# ── 3. Cargar límite Perú (unión de departamentos) ────────────────────────────
print('[02] Cargando límite nacional ...')
gdf_dept = gpd.read_file(SHP_DEPT)
if gdf_dept.crs != 'EPSG:4326':
    gdf_dept = gdf_dept.to_crs('EPSG:4326')
peru_wgs84  = unary_union(gdf_dept.geometry)
peru_utm    = gpd.GeoSeries([peru_wgs84], crs='EPSG:4326').to_crs('EPSG:32719').iloc[0]

# Buffer ligero para incluir estaciones costeras en el borde
peru_utm_buf = peru_utm.buffer(5000)   # 5 km
print(f'     Límite Perú UTM: bounds = {peru_utm.bounds}')

# ── 4. Calcular Voronoi ───────────────────────────────────────────────────────
print('[02] Calculando diagrama de Voronoi ...')
coords = np.array([[geom.x, geom.y] for geom in gdf_utm.geometry])

# Agregar puntos espejo en los 4 extremos del bounding box extendido
# para que todos los polígonos sean finitos
minx, miny, maxx, maxy = peru_utm_buf.bounds
margin = 500_000   # 500 km extra
mirror_pts = np.array([
    [minx - margin, miny - margin],
    [minx - margin, maxy + margin],
    [maxx + margin, miny - margin],
    [maxx + margin, maxy + margin],
    [(minx + maxx) / 2, miny - margin],
    [(minx + maxx) / 2, maxy + margin],
    [minx - margin, (miny + maxy) / 2],
    [maxx + margin, (miny + maxy) / 2],
])
all_pts = np.vstack([coords, mirror_pts])

vor = Voronoi(all_pts)

# ── 5. Construir polígonos finitos y recortar al Perú ────────────────────────
print('[02] Construyendo polígonos y recortando al Perú ...')

def voronoi_polygon(vor, point_idx):
    """Devuelve el polígono Voronoi para el punto original de índice point_idx."""
    region_idx = vor.point_region[point_idx]
    region     = vor.regions[region_idx]
    if -1 in region or len(region) == 0:
        return None
    try:
        return Polygon(vor.vertices[region])
    except Exception:
        return None

polys   = []
n_valid = 0
for i in range(len(df)):
    poly = voronoi_polygon(vor, i)
    if poly is None or not poly.is_valid:
        # Fallback: buffer del punto como zona de influencia mínima
        poly = gdf_utm.geometry.iloc[i].buffer(1000)
    try:
        clipped = poly.intersection(peru_utm_buf)
        if clipped.is_empty:
            clipped = gdf_utm.geometry.iloc[i].buffer(5000)
        polys.append(clipped)
        n_valid += 1
    except Exception:
        polys.append(gdf_utm.geometry.iloc[i].buffer(5000))

print(f'     Polígonos construidos: {n_valid} / {len(df)}')

# ── 6. Crear GeoDataFrame de salida ──────────────────────────────────────────
gdf_th = gdf_utm.copy()
gdf_th['geometry'] = polys
gdf_th = gdf_th.set_crs('EPSG:32719', allow_override=True)

# Columnas de metadatos que necesitan los scripts siguientes
KEEP = [
    'cod_qc', 'nombre', 'lat', 'lon',
    'altitud_oficial', 'altitud_dem', 'delta_z', 'clase_diagnostico',
    'dept', 'sector', 'cod_sector',
    'umbral_pp', 'umbral_t', 'radio_pp', 'radio_t',
    'geometry'
]
gdf_th = gdf_th[[c for c in KEEP if c in gdf_th.columns]]

# ── 7. Guardar ────────────────────────────────────────────────────────────────
out_gpkg = OUT / 'thiessen.gpkg'
gdf_th.to_file(str(out_gpkg), driver='GPKG', layer='thiessen_utm32719')
print(f'[02] Guardado (UTM 32719): {out_gpkg}')

# Versión WGS84 para referencia visual
gdf_th_wgs = gdf_th.to_crs('EPSG:4326')
gdf_th_wgs.to_file(str(out_gpkg), driver='GPKG', layer='thiessen_wgs84', mode='a')
print(f'[02] Guardado (WGS84 layer): {out_gpkg}  [layer=thiessen_wgs84]')

# Estadísticas rápidas
areas_km2 = (gdf_th.geometry.area / 1e6)
print(f'\n[02] Estadísticas de polígonos Thiessen (km²):')
print(f'     Min:    {areas_km2.min():.1f}')
print(f'     Median: {areas_km2.median():.1f}')
print(f'     Max:    {areas_km2.max():.1f}')
print(f'\n[02] Script 02 completado.')
