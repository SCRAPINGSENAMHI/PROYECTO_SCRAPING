"""
Script 05 — Integración Final y Generación de JSON para Dashboard
==================================================================
Metodología: PDF §6

Regla fundamental §6.1:
  El área representativa final = mínimo(radio_topográfico, radio_estadístico)
  NUNCA excede el radio_max de zona climática.

Para cada estación:
  1. Toma el polígono topográfico (Script 03)
  2. Aplica el radio estadístico (Script 04) — recorta si es menor
  3. Construye el JSON final compatible con el dashboard actual
  4. Agrega campos nuevos: sector, delta_z, clase_diagnostico, tipo_area
  5. Incluye vecinas con r_pp, r_temp, dist_km coloreadas según §9.2

Compatibilidad: los JSON son backward-compatible con server.py y el frontend.
Los campos nuevos se agregan sin romper campos existentes.

Salida: DATA/representatividad/ (reemplaza JSONs existentes)
        DATA/representatividad/_index.json
"""

import pathlib, warnings, json, math
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, mapping, Point
from shapely.ops import unary_union
import pyproj
warnings.filterwarnings('ignore')

# ── Rutas ─────────────────────────────────────────────────────────────────────
ROOT      = pathlib.Path(__file__).resolve().parent.parent
DATA      = ROOT / 'DATA'
OUT_V2    = DATA / 'representatividad_v2'
OUT_FINAL = DATA / 'representatividad'   # Carpeta del dashboard

PREP_CSV    = OUT_V2 / 'estaciones_prep.csv'
AREAS_CSV   = OUT_V2 / 'areas_topograficas.csv'
GEOJSON_DIR = OUT_V2 / 'geojsons'
PARES_CSV   = OUT_V2 / 'vecinas_correlaciones.csv'
RADIOS_CSV  = OUT_V2 / 'radios_estadisticos.csv'

OUT_FINAL.mkdir(exist_ok=True)

# ── Colores vecinas §9.2 ──────────────────────────────────────────────────────
def clasificar_r(r):
    if r is None or (isinstance(r, float) and math.isnan(r)):
        return 'sin_datos'
    r = float(r)
    if r >= 0.90: return 'muy_alta'
    if r >= 0.80: return 'alta'
    if r >= 0.70: return 'moderada'
    return 'baja'

# ── Tipo de área (§6) ─────────────────────────────────────────────────────────
def tipo_area(n_vecinas_r, radio_est):
    if n_vecinas_r == 0:
        return 'Topografica'
    if radio_est > 0:
        return 'Topografica+Estadistica'
    return 'Topografica'

# ── Recortar polígono al radio estadístico ────────────────────────────────────
def recortar_por_radio(geojson_str, lat, lon, radio_km, radio_est_km):
    """
    Si radio_est_km < radio_km, recorta el polígono WGS84
    a un círculo de radio_est_km en torno a la estación.
    """
    if geojson_str is None:
        return None, radio_km
    if radio_est_km >= radio_km or radio_est_km <= 0:
        return geojson_str, radio_km

    try:
        # Crear círculo en UTM 32719 y reproyectar a WGS84
        transformer = pyproj.Transformer.from_crs('EPSG:4326', 'EPSG:32719', always_xy=True)
        x, y = transformer.transform(lon, lat)
        circle_utm = Point(x, y).buffer(radio_est_km * 1000)

        gdf = gpd.GeoSeries(
            [shape(json.loads(geojson_str))], crs='EPSG:4326'
        ).to_crs('EPSG:32719')

        circle_gdf = gpd.GeoSeries([circle_utm], crs='EPSG:32719')
        clipped    = gdf.iloc[0].intersection(circle_gdf.iloc[0])

        if clipped.is_empty:
            return geojson_str, radio_km

        clipped_wgs = gpd.GeoSeries([clipped], crs='EPSG:32719').to_crs('EPSG:4326').iloc[0]
        return json.dumps(mapping(clipped_wgs)), radio_est_km

    except Exception:
        return geojson_str, radio_km

# ── Main ──────────────────────────────────────────────────────────────────────
print('[05] Cargando datos ...')
df_prep   = pd.read_csv(PREP_CSV)
df_radios = pd.read_csv(RADIOS_CSV)
df_pares  = pd.read_csv(PARES_CSV)

# Cargar CSV de áreas (sin geojson) + leer geojsons individuales de GEOJSON_DIR
if not AREAS_CSV.exists():
    print('     ADVERTENCIA: areas_topograficas.csv no encontrado.')
    print('     Ejecuta primero el Script 03.')
    exit(1)
df_areas = pd.read_csv(AREAS_CSV)
print(f'     Áreas topográficas (CSV): {len(df_areas)} estaciones')
print(f'     GeoJSONs individuales en: {GEOJSON_DIR}  ({len(list(GEOJSON_DIR.glob("*.json")))} archivos)')

print(f'     Radios estadísticos: {len(df_radios)} estaciones')
print(f'     Pares vecinas: {len(df_pares)} pares')

# Índice de radios estadísticos
radios_idx = df_radios.set_index('cod_qc').to_dict('index')

# Índice de vecinas por estación objetivo
vecinas_idx = {}
for cod_obj, grupo in df_pares.groupby('cod_qc_obj'):
    vecinas_idx[cod_obj] = grupo.to_dict('records')

# Índice rápido de coordenadas por cod_qc
prep_idx = df_prep.set_index('cod_qc')[['lat','lon','altitud_oficial']].to_dict('index')

# ── Construir JSON por estación ───────────────────────────────────────────────
print('[05] Generando JSON por estación ...', flush=True)
index_entries = []
generados = 0
errores   = 0

for _, row in df_areas.iterrows():
    cod_qc = row.get('cod_qc', '')
    if not cod_qc:
        continue

    lat = float(row.get('lat', 0))
    lon = float(row.get('lon', 0))
    radio_pp   = float(row.get('radio_pp', 30))
    radio_t    = float(row.get('radio_t',  60))

    # Radios estadísticos (§6.1)
    rinfo = radios_idx.get(cod_qc, {})
    radio_est_pp   = float(rinfo.get('radio_est_pp_km',   radio_pp))
    radio_est_temp = float(rinfo.get('radio_est_temp_km', radio_t))
    n_vec_r        = int(rinfo.get('n_vecinas_r_080', 0))
    inhomog        = bool(rinfo.get('inhomogeneidad', False))

    # Regla fundamental §6.1: radio final = min(topográfico, estadístico)
    # nunca excede radio_max de zona
    radio_final_pp   = min(radio_pp,   radio_est_pp   if radio_est_pp   > 0 else radio_pp)
    radio_final_temp = min(radio_t,    radio_est_temp if radio_est_temp > 0 else radio_t)

    # Leer GeoJSONs del archivo individual del Script 03
    gj_path = GEOJSON_DIR / f'{cod_qc}.json'
    geojson_pp_str   = None
    geojson_temp_str = None
    if gj_path.exists():
        try:
            gj_data = json.loads(gj_path.read_text(encoding='utf-8'))
            geojson_pp_str   = gj_data.get('geojson_pp')
            geojson_temp_str = gj_data.get('geojson_temp')
        except Exception:
            pass

    area_pp_km2  = row.get('area_pp_km2', 0) or 0
    radio_pp_km  = row.get('radio_pp_km', 0) or 0
    area_temp_km2 = row.get('area_temp_km2', 0) or 0
    radio_temp_km = row.get('radio_temp_km', 0) or 0

    if geojson_pp_str and radio_est_pp < radio_pp:
        geojson_pp_str, _ = recortar_por_radio(
            geojson_pp_str, lat, lon, radio_pp, radio_est_pp
        )

    if geojson_temp_str and radio_est_temp < radio_t:
        geojson_temp_str, _ = recortar_por_radio(
            geojson_temp_str, lat, lon, radio_t, radio_est_temp
        )

    # Vecinas (§9.2)
    vecinas_raw = vecinas_idx.get(cod_qc, [])
    vecinas_out = []
    for v in vecinas_raw[:6]:
        r_pp_v   = v.get('r_pp')
        r_temp_v = v.get('r_temp')
        if isinstance(r_pp_v, float) and math.isnan(r_pp_v):
            r_pp_v = None
        vi = prep_idx.get(v['cod_qc_vecina'], {})
        vecinas_out.append({
            'cod_qc':    v['cod_qc_vecina'],
            'nombre':    v['nombre_vecina'],
            'lat':       float(vi.get('lat', 0) or 0),
            'lon':       float(vi.get('lon', 0) or 0),
            'altitud':   float(v.get('altitud_vec', 0)) if pd.notna(v.get('altitud_vec')) else 0,
            'dist_km':   round(float(v['dist_km']), 2),
            'r_pp':      round(float(r_pp_v), 3) if r_pp_v is not None else None,
            'r_temp':    round(float(r_temp_v), 3) if r_temp_v is not None and not math.isnan(float(r_temp_v)) else None,
            'dept':      v.get('dept_vecina', ''),
            'sector':    v.get('sector_vecina', ''),
            'mismo_sector': bool(v.get('mismo_sector', False)),
            'clase_r':   clasificar_r(r_pp_v),
        })

    # Zona / tipo para compatibilidad con dashboard actual
    sector = str(row.get('sector', 'SIERRA CENTRAL OCCIDENTAL'))
    zona_legacy = 'Sierra / Selva alta'
    s_up = sector.upper()
    if 'COSTA' in s_up:      zona_legacy = 'Costa'
    elif 'SELVA' in s_up and 'BAJA' in s_up: zona_legacy = 'Selva baja'
    elif 'SELVA' in s_up:    zona_legacy = 'Selva alta'
    elif 'SIERRA' in s_up:   zona_legacy = 'Sierra / Selva alta'

    # Construir JSON final (backward-compatible §9.1)
    doc = {
        'cod_qc':  cod_qc,
        'nombre':  str(row.get('nombre', '')),
        'lat':     lat,
        'lon':     lon,
        'altitud': float(row.get('altitud_oficial', 0)) if pd.notna(row.get('altitud_oficial')) else 0,
        'dept':    str(row.get('dept', '')),
        'zona':    zona_legacy,
        # Campos nuevos §6.3
        'sector':            sector,
        'delta_z':           round(float(row.get('delta_z', 0)), 1) if pd.notna(row.get('delta_z')) else None,
        'clase_diagnostico': str(row.get('clase_diagnostico', '')),
        'inhomogeneidad':    inhomog,
        'tipo_area':         tipo_area(n_vec_r, radio_est_pp),
        # PP
        'pp': {
            'umbral_m':    int(row.get('umbral_pp', 100)),
            'radio_max':   int(radio_pp),
            'radio_est':   round(radio_est_pp, 2),
            'radio_final': round(radio_final_pp, 2),
            'area_km2':    round(float(area_pp_km2), 2) if pd.notna(area_pp_km2) else 0,
            'radio_km':    round(float(radio_pp_km), 2)  if pd.notna(radio_pp_km) else 0,
            'geojson':     json.loads(geojson_pp_str) if geojson_pp_str else None,
        },
        # Temperatura
        'temp': {
            'umbral_m':    int(row.get('umbral_t', 200)),
            'radio_max':   int(radio_t),
            'radio_est':   round(radio_est_temp, 2),
            'radio_final': round(radio_final_temp, 2),
            'area_km2':    round(float(area_temp_km2), 2) if pd.notna(area_temp_km2) else 0,
            'radio_km':    round(float(radio_temp_km), 2) if pd.notna(radio_temp_km) else 0,
            'geojson':     json.loads(geojson_temp_str) if geojson_temp_str else None,
        },
        'vecinas': vecinas_out,
    }

    # Guardar JSON individual
    out_path = OUT_FINAL / f'{cod_qc}.json'
    try:
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, separators=(',', ':'))
        generados += 1
    except Exception as e:
        errores += 1

    index_entries.append({
        'cod_qc': cod_qc,
        'nombre': doc['nombre'],
        'lat':    lat,
        'lon':    lon,
        'dept':   doc['dept'],
        'sector': sector,
        'area_pp_km2':  doc['pp']['area_km2'],
        'radio_pp_km':  doc['pp']['radio_km'],
        'tipo_area':    doc['tipo_area'],
    })

    if generados % 100 == 0:
        print(f'     {generados} JSON generados ...')

# ── Guardar índice ────────────────────────────────────────────────────────────
idx_path = OUT_FINAL / '_index.json'
with open(idx_path, 'w', encoding='utf-8') as f:
    json.dump(index_entries, f, ensure_ascii=False, separators=(',', ':'))

print(f'\n[05] JSON generados: {generados}')
print(f'[05] Errores: {errores}')
print(f'[05] Índice guardado: {idx_path}')
print(f'\n[05] Script 05 completado.')
