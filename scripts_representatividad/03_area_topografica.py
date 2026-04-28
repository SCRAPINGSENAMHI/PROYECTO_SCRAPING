"""
Script 03 — Área Representativa Topográfica (v3 — optimizado)
==============================================================
Metodología: PDF §4 · PRISM (Daly 1994,2002) · OMM-Nº 168 §2.4.2

Optimizaciones clave:
  - Filtro altimétrico + distancia en 30 m (preciso)
  - Vectorización sobre máscara reducida a 300 m (100× menos píxeles)
    → válido porque el PDF §4.3 paso 13 pide simplificar a ≤ 500 m
  - Cada GeoJSON se escribe en disco durante el proceso (sin acumular RAM)
  - Si ya existe el archivo JSON de una estación, se salta (reanudable)

Salida por estación: DATA/representatividad_v2/geojsons/<cod_qc>.json
Salida resumen    : DATA/representatividad_v2/areas_topograficas.csv
"""

import pathlib, warnings, time, json, sys
import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window
from rasterio.transform import rowcol
from rasterio.features import shapes as rio_shapes
from shapely.geometry import shape, mapping, Point
from shapely.ops import unary_union
import geopandas as gpd
from pyproj import Transformer
warnings.filterwarnings('ignore')

# ── Rutas ─────────────────────────────────────────────────────────────────────
ROOT        = pathlib.Path(__file__).resolve().parent.parent
DATA        = ROOT / 'DATA'
OUT         = DATA / 'representatividad_v2'
GEOJSON_DIR = OUT / 'geojsons'
DEM_PATH    = DATA / 'DEM' / 'DEM_FINAL.tif'
PREP_CSV    = OUT / 'estaciones_prep.csv'

OUT.mkdir(exist_ok=True)
GEOJSON_DIR.mkdir(exist_ok=True)

T_WGS_UTM = Transformer.from_crs('EPSG:4326', 'EPSG:32719', always_xy=True)

FACTOR_REDUC = 10   # 30m → 300m para vectorización

def tol_simplif(sector):
    return 200 if 'SIERRA' in str(sector).upper() else 500

def calcular_area_estacion(row, src_dem):
    lat  = float(row['lat'])
    lon  = float(row['lon'])
    z_ok = pd.notna(row.get('altitud_oficial'))
    z_stn = float(row['altitud_oficial']) if z_ok else float(row.get('altitud_dem', 0) or 0)

    x_stn, y_stn = T_WGS_UTM.transform(lon, lat)

    res_x    = abs(src_dem.transform.a)   # 30 m
    res_y    = abs(src_dem.transform.e)   # 30 m
    dem_nd   = src_dem.nodata or 3.4e38

    results = {}

    for var in ('pp', 'temp'):
        umbral  = float(row['umbral_pp'] if var == 'pp' else row['umbral_t'])
        radio_m = float(row['radio_pp']  if var == 'pp' else row['radio_t']) * 1000

        try:
            # ── 1. Ventana rectangular al radio_max ─────────────────────────
            npix = int(np.ceil(radio_m / res_x)) + 2
            row_c, col_c = rowcol(src_dem.transform, x_stn, y_stn)
            col0 = max(0, col_c - npix);  col1 = min(src_dem.width,  col_c + npix + 1)
            row0 = max(0, row_c - npix);  row1 = min(src_dem.height, row_c + npix + 1)
            ww = col1 - col0;  wh = row1 - row0
            if ww <= 0 or wh <= 0:
                results[var] = None; continue

            # ── 2. Leer DEM a 30 m ─────────────────────────────────────────
            dem_win = src_dem.read(1, window=Window(col0, row0, ww, wh)).astype(np.float32)
            win_tr  = src_dem.window_transform(Window(col0, row0, ww, wh))

            # Coordenadas UTM de centros de píxel
            ci, ri = np.meshgrid(np.arange(ww), np.arange(wh))
            x_arr  = win_tr.c + (ci + 0.5) * win_tr.a
            y_arr  = win_tr.f + (ri + 0.5) * win_tr.e
            dist_m = np.sqrt((x_arr - x_stn)**2 + (y_arr - y_stn)**2)

            nd_mask = (dem_win >= dem_nd * 0.9) | (~np.isfinite(dem_win)) | (dem_win < -500)

            # ── 3. DOBLE FILTRO (§4.3 pasos 9-11) — a 30 m ─────────────────
            mask30 = (np.abs(dem_win - z_stn) <= umbral) & (dist_m <= radio_m) & (~nd_mask)

            n_valid = int(mask30.sum())
            area_km2 = n_valid * res_x * res_y / 1e6
            radio_km  = round(float(np.sqrt(area_km2 / np.pi)), 3)

            if n_valid == 0:
                # Círculo mínimo de 2 km
                geom_wgs = gpd.GeoSeries(
                    [Point(x_stn, y_stn).buffer(2000)], crs='EPSG:32719'
                ).to_crs('EPSG:4326').iloc[0]
                results[var] = {
                    'geojson': json.dumps(mapping(geom_wgs)),
                    'area_km2': round(4 * np.pi / 1e6 * 1e6, 4),
                    'radio_km': 2.0, 'n_pixels': 0,
                }
                continue

            # ── 4. Reducir máscara a 300 m para vectorizar (§4.3 paso 12) ──
            # Recorta al múltiplo exacto de FACTOR_REDUC
            H30 = (wh // FACTOR_REDUC) * FACTOR_REDUC
            W30 = (ww // FACTOR_REDUC) * FACTOR_REDUC
            m30 = mask30[:H30, :W30].astype(np.uint8)

            # Reshape + any(): bloque de 10×10 px→1 px de 300 m
            m300 = m30.reshape(H30 // FACTOR_REDUC, FACTOR_REDUC,
                               W30 // FACTOR_REDUC, FACTOR_REDUC)
            m300 = m300.any(axis=(1, 3)).astype(np.uint8)

            # Transform para la grilla de 300 m
            res300_x = win_tr.a * FACTOR_REDUC
            res300_y = win_tr.e * FACTOR_REDUC
            tr300 = win_tr * win_tr.scale(FACTOR_REDUC, FACTOR_REDUC)

            # ── 5. Vectorizar grilla de 300 m ───────────────────────────────
            geom_list = [
                shape(g) for g, v in rio_shapes(m300, transform=tr300)
                if v == 1 and shape(g).is_valid
            ]

            if not geom_list:
                # Fallback: buffer circular
                geom_wgs = gpd.GeoSeries(
                    [Point(x_stn, y_stn).buffer(radio_m * 0.9)], crs='EPSG:32719'
                ).to_crs('EPSG:4326').iloc[0]
                results[var] = {
                    'geojson': json.dumps(mapping(geom_wgs)),
                    'area_km2': round(area_km2, 4),
                    'radio_km': radio_km, 'n_pixels': n_valid,
                }
                continue

            # ── 6. Unir y simplificar (§4.3 pasos 13-14) ───────────────────
            union_geom = unary_union(geom_list)
            tol = tol_simplif(row.get('sector', ''))
            union_geom = union_geom.simplify(tol, preserve_topology=True)

            # Reproyectar a WGS84
            geom_wgs = gpd.GeoSeries(
                [union_geom], crs='EPSG:32719'
            ).to_crs('EPSG:4326').iloc[0]

            results[var] = {
                'geojson':  json.dumps(mapping(geom_wgs)),
                'area_km2': round(area_km2, 4),
                'radio_km': radio_km,
                'n_pixels': n_valid,
            }

        except Exception as e:
            results[var] = None

    return results

# ── Main ──────────────────────────────────────────────────────────────────────
print('[03] Cargando estaciones_prep.csv ...')
df = pd.read_csv(PREP_CSV).dropna(subset=['lat', 'lon']).reset_index(drop=True)
print(f'     {len(df)} estaciones')

# Verificar cuántas ya están procesadas (reanudable)
ya_procesadas = {p.stem for p in GEOJSON_DIR.glob('*.json')}
pendientes    = df[~df['cod_qc'].isin(ya_procesadas)]
print(f'     Ya procesadas: {len(ya_procesadas)}  |  Pendientes: {len(pendientes)}')

t0 = time.time()
records_meta = []
errores = 0

with rasterio.open(DEM_PATH) as src_dem:
    print(f'[03] DEM: {src_dem.crs}  res={src_dem.res}  shape={src_dem.shape}')

    for idx, (_, row) in enumerate(pendientes.iterrows()):
        cod_qc = row['cod_qc']
        res = calcular_area_estacion(row, src_dem)

        rec = {
            'cod_qc': cod_qc, 'nombre': row['nombre'],
            'lat': row['lat'], 'lon': row['lon'],
            'altitud_oficial': row.get('altitud_oficial'),
            'dept': row.get('dept', ''), 'sector': row.get('sector', ''),
            'umbral_pp': row.get('umbral_pp'), 'umbral_t': row.get('umbral_t'),
            'radio_pp': row.get('radio_pp'),   'radio_t':  row.get('radio_t'),
            'delta_z': row.get('delta_z'),
            'clase_diagnostico': row.get('clase_diagnostico', ''),
        }

        doc_geo = {}
        if res:
            for var, key in [('pp', 'pp'), ('temp', 'temp')]:
                v = res.get(var)
                if v:
                    rec[f'area_{key}_km2'] = v['area_km2']
                    rec[f'radio_{key}_km'] = v['radio_km']
                    rec[f'n_px_{key}']     = v['n_pixels']
                    doc_geo[f'geojson_{key}'] = v['geojson']
                else:
                    errores += 1
                    rec[f'area_{key}_km2'] = None
                    rec[f'radio_{key}_km'] = None
                    rec[f'n_px_{key}']     = 0
        else:
            errores += 1
            for key in ('pp', 'temp'):
                rec[f'area_{key}_km2'] = None
                rec[f'radio_{key}_km'] = None
                rec[f'n_px_{key}']     = 0

        # Guardar GeoJSON individualmente (sin acumular en RAM)
        gj_path = GEOJSON_DIR / f'{cod_qc}.json'
        with open(gj_path, 'w', encoding='utf-8') as f:
            json.dump(doc_geo, f, ensure_ascii=False, separators=(',', ':'))

        records_meta.append(rec)

        if (idx + 1) % 50 == 0:
            elapsed = time.time() - t0
            eta     = elapsed / (idx + 1) * (len(pendientes) - idx - 1)
            print(f'     {idx+1}/{len(pendientes)} '
                  f'elapsed={elapsed:.0f}s  ETA={eta:.0f}s  err={errores}', flush=True)

elapsed_total = time.time() - t0

# ── Guardar CSV de metadatos ───────────────────────────────────────────────────
df_meta = pd.DataFrame(records_meta)
out_csv = OUT / 'areas_topograficas.csv'

# Si hay run anterior, combinar
if out_csv.exists() and len(ya_procesadas) > 0:
    df_prev = pd.read_csv(out_csv)
    df_meta = pd.concat([df_prev, df_meta], ignore_index=True) \
                .drop_duplicates(subset='cod_qc', keep='last')

df_meta.to_csv(out_csv, index=False, encoding='utf-8')
print(f'\n[03] CSV guardado: {out_csv}  ({len(df_meta)} filas)')
print(f'[03] GeoJSONs: {len(list(GEOJSON_DIR.glob("*.json")))} archivos en {GEOJSON_DIR}')
print(f'[03] Errores: {errores}  |  Tiempo: {elapsed_total:.0f}s')

# Stats
if not df_meta.empty:
    print('\n[03] Stats area PP (km2):')
    print(df_meta['area_pp_km2'].describe().round(1).to_string())
print(f'\n[03] Script 03 completado.')
sys.stdout.flush()
