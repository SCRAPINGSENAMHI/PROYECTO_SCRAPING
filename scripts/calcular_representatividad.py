"""
calcular_representatividad.py
Ejecutar UNA sola vez. Genera DATA/representatividad/<cod_qc>.json
para cada estación histórica con:
  - Polígono GeoJSON del área representativa (PP y T°) derivado del DEM
  - Lista de 6 estaciones vecinas con distancia y correlación Pearson
"""

import sys, json, math, pathlib, importlib.util, warnings
warnings.filterwarnings('ignore')

import numpy as np
import pandas as pd
from pyproj import Transformer
from rasterio.windows import from_bounds
from rasterio.features import shapes as rio_shapes
from shapely.geometry import shape, mapping
from shapely.ops import unary_union, transform as shp_transform
from scipy.spatial import KDTree
from scipy.stats import pearsonr
import rasterio

# ── Rutas ────────────────────────────────────────────────────────────────────
ROOT      = pathlib.Path(__file__).resolve().parent.parent
DEM_PATH  = ROOT / 'DATA' / 'DEM' / 'DEM_FINAL.tif'
CSV_HIST  = ROOT / 'DATA' / 'stations_hist_portal.csv'
HIST_DIR  = ROOT / 'DATA' / 'HISTORICA'
OUT_DIR   = ROOT / 'DATA' / 'representatividad'
OUT_DIR.mkdir(exist_ok=True)

# ── Cargar parse_hist_txt desde app.py ──────────────────────────────────────
def _load_app():
    spec = importlib.util.spec_from_file_location('app_mod', ROOT / 'app' / 'app.py')
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

print("Cargando módulo app...", flush=True)
app_mod = _load_app()

# ── Parámetros WMO / Perú ────────────────────────────────────────────────────
PARAMS = {
    'sierra': {   # alt >= 800 m
        'pp':   {'umbral_m': 100, 'radio_m': 15_000,  'simplify_m': 300},
        'temp': {'umbral_m': 200, 'radio_m': 30_000,  'simplify_m': 500},
    },
    'llano': {    # alt < 800 m  — simplificación agresiva para reducir JSON
        'pp':   {'umbral_m': 100, 'radio_m': 50_000,  'simplify_m': 2000},
        'temp': {'umbral_m': 200, 'radio_m': 100_000, 'simplify_m': 4000},
    },
}
ZONA_LABELS = {
    'sierra': 'Sierra / Selva alta',
    'llano':  'Costa / Selva baja',
}
N_VECINAS    = 6
MIN_MESES    = 24   # mínimo meses en común para calcular correlación

# ── Reproyección ─────────────────────────────────────────────────────────────
to_utm   = Transformer.from_crs('EPSG:4326', 'EPSG:32719', always_xy=True)
to_wgs84 = Transformer.from_crs('EPSG:32719', 'EPSG:4326', always_xy=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
def zona(altitud):
    return 'sierra' if (altitud is not None and float(altitud) >= 800) else 'llano'

def _buscar_txt(cod_qc):
    """Busca qcXXXXXX.txt en todos los subdirectorios de HIST_DIR."""
    for dept_dir in HIST_DIR.iterdir():
        f = dept_dir / f'{cod_qc}.txt'
        if f.exists():
            return f
    return None

def cargar_serie(cod_qc):
    """Retorna DataFrame mensual con cols [precip_mm, tmax_c] o None."""
    f = _buscar_txt(cod_qc)
    if f is None:
        return None
    try:
        txt = f.read_text(encoding='latin-1', errors='ignore')
        df  = app_mod.parse_hist_txt(txt)
        if df is None or df.empty:
            return None
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
        df = df.dropna(subset=['fecha']).set_index('fecha')
        # Resamplear a media mensual (reduce ruido y gaps)
        monthly = df[['precip_mm','tmax_c']].resample('MS').mean()
        return monthly
    except Exception as e:
        print(f"  [WARN] {cod_qc}: {e}", flush=True)
        return None

def calcular_poligono(dem, x_utm, y_utm, alt_est, var_params):
    """
    Recorta ventana del DEM, aplica máscaras de altitud y distancia,
    convierte a polígono GeoJSON (WGS84) y calcula área.
    Retorna dict {geojson, area_km2, radio_km} o None si falla.
    """
    radio_m   = var_params['radio_m']
    umbral_m  = var_params['umbral_m']
    simplify  = var_params['simplify_m']

    try:
        win = from_bounds(
            x_utm - radio_m, y_utm - radio_m,
            x_utm + radio_m, y_utm + radio_m,
            dem.transform
        )
        data = dem.read(1, window=win, boundless=True, fill_value=-9999)
        wt   = dem.window_transform(win)

        nrows, ncols = data.shape
        # Máscara altitud
        m_alt  = (np.abs(data.astype(float) - alt_est) <= umbral_m) & (data > -9000)
        # Máscara distancia (círculo)
        res_m  = abs(dem.transform.a)  # metros/pixel
        rows_i, cols_i = np.ogrid[:nrows, :ncols]
        cx, cy = ncols // 2, nrows // 2
        dist   = np.sqrt(((cols_i - cx) * res_m)**2 + ((rows_i - cy) * res_m)**2)
        m_dist = dist <= radio_m

        mascara = (m_alt & m_dist).astype(np.uint8)
        n_px    = int(mascara.sum())
        if n_px == 0:
            return None

        area_km2 = round(n_px * (res_m**2) / 1e6, 2)
        radio_km = round(math.sqrt(area_km2 / math.pi), 2)

        # Píxeles → polígonos en CRS del DEM (UTM)
        polys_utm = [shape(g) for g, v in rio_shapes(mascara, transform=wt) if int(v) == 1]
        if not polys_utm:
            return None

        union_utm = unary_union(polys_utm).simplify(simplify, preserve_topology=True)

        # Reproyectar UTM → WGS84
        def utm_to_wgs(x_arr, y_arr):
            lons, lats = to_wgs84.transform(x_arr, y_arr)
            return lons, lats

        union_wgs = shp_transform(utm_to_wgs, union_utm)

        # Simplificar un poco más en grados (≈ simplify_m / 111000 °)
        union_wgs = union_wgs.simplify(simplify / 111_000, preserve_topology=True)

        return {
            'geojson':  mapping(union_wgs),
            'area_km2': area_km2,
            'radio_km': radio_km,
        }
    except Exception as e:
        print(f"  [WARN] polígono error: {e}", flush=True)
        return None

# ── Cargar estaciones históricas ──────────────────────────────────────────────
print("Cargando estaciones históricas...", flush=True)
df = pd.read_csv(CSV_HIST)
df = df.dropna(subset=['lat','lon'])
df['altitud'] = pd.to_numeric(df['altitud'], errors='coerce').fillna(500)
df = df.reset_index(drop=True)
print(f"  {len(df)} estaciones con coordenadas", flush=True)

# Coordenadas UTM para KDTree
coords_utm = np.array([to_utm.transform(row.lon, row.lat) for _, row in df.iterrows()])
tree = KDTree(coords_utm)

# Pre-cargar todas las series mensuales (caché en memoria)
print("Pre-cargando series históricas...", flush=True)
series_cache = {}
for _, row in df.iterrows():
    cod = str(row.get('cod_qc', '') or '').strip()
    if cod:
        series_cache[cod] = cargar_serie(cod)
n_con_serie = sum(1 for v in series_cache.values() if v is not None)
print(f"  {n_con_serie}/{len(df)} estaciones con datos válidos", flush=True)

# ── Proceso principal ─────────────────────────────────────────────────────────
print(f"\nProcesando {len(df)} estaciones...", flush=True)
print(f"Resultados en: {OUT_DIR}\n", flush=True)

with rasterio.open(DEM_PATH) as dem:
    for idx, row in df.iterrows():
        cod_qc  = str(row.get('cod_qc', '') or '').strip()
        nombre  = str(row.get('estacion', '') or f'STN_{idx}').strip()
        lat, lon = float(row.lat), float(row.lon)
        alt_est  = float(row.altitud)
        dept     = str(row.get('departamento', '') or '').strip()
        z        = zona(alt_est)
        x_utm, y_utm = to_utm.transform(lon, lat)

        print(f"[{idx+1:3d}/{len(df)}] {nombre:30s} | {z:7s} | alt={alt_est:5.0f}m", end=' ', flush=True)

        # ── Polígonos PP y T° ───────────────────────────────────────────────
        poly_pp   = calcular_poligono(dem, x_utm, y_utm, alt_est, PARAMS[z]['pp'])
        poly_temp = calcular_poligono(dem, x_utm, y_utm, alt_est, PARAMS[z]['temp'])

        # ── Vecinas ─────────────────────────────────────────────────────────
        dists_utm, idxs = tree.query([x_utm, y_utm], k=N_VECINAS + 1)
        vecinas = []
        serie_central = series_cache.get(cod_qc)

        for dist_m, vi in zip(dists_utm[1:], idxs[1:]):
            vrow  = df.iloc[vi]
            v_cod = str(vrow.get('cod_qc', '') or '').strip()
            dist_km = round(float(dist_m) / 1000, 2)

            r_pp   = None
            r_temp = None

            if serie_central is not None and v_cod and v_cod in series_cache:
                serie_vec = series_cache[v_cod]
                if serie_vec is not None:
                    try:
                        comun = serie_central[['precip_mm']].join(
                            serie_vec[['precip_mm']].rename(columns={'precip_mm':'pp_v'}),
                            how='inner'
                        ).dropna()
                        if len(comun) >= MIN_MESES:
                            r, _ = pearsonr(comun['precip_mm'], comun['pp_v'])
                            r_pp = round(float(r), 3) if not math.isnan(r) else None
                    except Exception:
                        pass
                    try:
                        comun_t = serie_central[['tmax_c']].join(
                            serie_vec[['tmax_c']].rename(columns={'tmax_c':'t_v'}),
                            how='inner'
                        ).dropna()
                        if len(comun_t) >= MIN_MESES:
                            r, _ = pearsonr(comun_t['tmax_c'], comun_t['t_v'])
                            r_temp = round(float(r), 3) if not math.isnan(r) else None
                    except Exception:
                        pass

            vecinas.append({
                'cod_qc':   v_cod,
                'nombre':   str(vrow.get('estacion', f'STN_{vi}')).strip(),
                'lat':      round(float(vrow.lat), 5),
                'lon':      round(float(vrow.lon), 5),
                'altitud':  round(float(vrow.altitud), 0),
                'dist_km':  dist_km,
                'r_pp':     r_pp,
                'r_temp':   r_temp,
                'dept':     str(vrow.get('departamento', '') or '').strip(),
            })

        # ── Guardar JSON ─────────────────────────────────────────────────────
        resultado = {
            'cod_qc':   cod_qc,
            'nombre':   nombre,
            'lat':      round(lat, 5),
            'lon':      round(lon, 5),
            'altitud':  round(alt_est, 0),
            'dept':     dept,
            'zona':     ZONA_LABELS[z],
            'pp': {
                'umbral_m':  PARAMS[z]['pp']['umbral_m'],
                'radio_max': PARAMS[z]['pp']['radio_m'] // 1000,
                'area_km2':  poly_pp['area_km2']   if poly_pp  else None,
                'radio_km':  poly_pp['radio_km']   if poly_pp  else None,
                'geojson':   poly_pp['geojson']    if poly_pp  else None,
            },
            'temp': {
                'umbral_m':  PARAMS[z]['temp']['umbral_m'],
                'radio_max': PARAMS[z]['temp']['radio_m'] // 1000,
                'area_km2':  poly_temp['area_km2'] if poly_temp else None,
                'radio_km':  poly_temp['radio_km'] if poly_temp else None,
                'geojson':   poly_temp['geojson']  if poly_temp else None,
            },
            'vecinas': vecinas,
        }

        out_file = OUT_DIR / f'{cod_qc}.json'
        with open(out_file, 'w', encoding='utf-8') as fout:
            json.dump(resultado, fout, ensure_ascii=False, separators=(',', ':'))

        area_pp_str   = f"{poly_pp['area_km2']:7.1f} km2"  if poly_pp   else "  sin datos "
        area_tmp_str  = f"{poly_temp['area_km2']:7.1f} km2" if poly_temp else "  sin datos "
        print(f"PP={area_pp_str}  T={area_tmp_str}  OK", flush=True)

print(f"\nListo. {len(df)} JSONs en {OUT_DIR}", flush=True)
