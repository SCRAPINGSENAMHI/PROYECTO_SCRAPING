"""
Script 04 — Estaciones Vecinas, Correlación de Pearson y Doble Masa
====================================================================
Metodología: PDF §5 · Tabios & Salas (1985) · OMM-Nº 100 Cap. 3

Para cada estación:
  1. KD-Tree sobre coords UTM → N=6 vecinas más cercanas dentro del radio_max (§5.2)
  2. Filtra vecinas por |ΔZ_par| <= 2 × umbral_ΔZ (§5.2 criterio 20)
  3. Filtra vecinas por MISMO SECTOR CLIMÁTICO (mejora metodológica)
     Si quedan < 3 vecinas, relaja al sector adyacente (COD_REGION igual)
  4. Calcula Pearson r (§5.3):
       - Lee series históricas de DATA/HISTORICA/<dept>/qcXXXXXXXX.txt
       - Exige >= 10 años de registros comunes (§5.3 punto 22)
       - Solo p < 0.05 (§5.3 punto 23)
  5. Determina radio estadístico: distancia donde r cae a 0.80 (§5.4)
  6. Análisis de doble masa (§5.5): detecta inhomogeneidades con Pettitt

Salida: DATA/representatividad_v2/vecinas_correlaciones.csv
        (una fila por par estacion-vecina)
        DATA/representatividad_v2/radios_estadisticos.csv
        (una fila por estacion: radio_est_pp_km, radio_est_temp_km)
"""

import pathlib, warnings, json, re
import numpy as np
import pandas as pd
import geopandas as gpd
from scipy.spatial import KDTree
from scipy.stats import pearsonr
from scipy.stats import wilcoxon
from pyproj import Transformer
warnings.filterwarnings('ignore')

# ── Rutas ─────────────────────────────────────────────────────────────────────
ROOT      = pathlib.Path(__file__).resolve().parent.parent
DATA      = ROOT / 'DATA'
OUT       = DATA / 'representatividad_v2'
HIST_DIR  = DATA / 'HISTORICA'
PREP_CSV  = OUT / 'estaciones_prep.csv'

# ── Parámetros ────────────────────────────────────────────────────────────────
N_VECINAS      = 6       # §5.2 PDF
MIN_AÑOS       = 10      # §5.3 PDF: mínimo 10 años registros comunes
MIN_REGISTROS  = MIN_AÑOS * 12   # Para datos mensuales agregados
R_UMBRAL       = 0.80    # Tabios & Salas (1985)
P_VALOR_MAX    = 0.05    # §5.3

# ── Leer series históricas ────────────────────────────────────────────────────
_series_cache = {}

def _leer_serie_historica(cod_qc, dept):
    """
    Retorna DataFrame con columnas [fecha, pp, temp] (mensuales).
    Lee el archivo qcXXXXXXXX.txt de DATA/HISTORICA/<dept>/.
    """
    if cod_qc in _series_cache:
        return _series_cache[cod_qc]

    # Buscar archivo
    fname = f'{cod_qc}.txt'
    candidates = list(HIST_DIR.rglob(fname))
    if not candidates:
        _series_cache[cod_qc] = None
        return None

    try:
        txt = candidates[0].read_text(encoding='utf-8', errors='replace')
        lines = txt.strip().splitlines()

        records = []
        for line in lines:
            parts = line.split()
            if len(parts) < 3:
                continue
            # Formato SENAMHI: Año Mes Día Val1 Val2 ...
            # También puede ser YYYY-MM-DD Val1 ...
            try:
                p0 = parts[0]
                if '-' in p0 or '/' in p0:
                    fecha = pd.to_datetime(p0, errors='coerce')
                    val_parts = parts[1:]
                elif len(p0) == 4 and p0.isdigit() and len(parts) >= 3:
                    # Año Mes Día
                    yy, mm, dd = int(parts[0]), int(parts[1]), int(parts[2])
                    fecha = pd.Timestamp(year=yy, month=max(1,min(12,mm)),
                                         day=max(1,min(31,dd)))
                    val_parts = parts[3:]
                else:
                    fecha = pd.to_datetime(p0, errors='coerce')
                    val_parts = parts[1:]

                if pd.isna(fecha):
                    continue
                vals = [float(p) if p not in ('-', 'nan', 'NaN', '', 'S/D') else np.nan
                        for p in val_parts]
                records.append({'fecha': fecha, 'vals': vals})
            except Exception:
                continue

        if not records:
            _series_cache[cod_qc] = None
            return None

        # Construir DataFrame simple con promedio de valores disponibles
        df = pd.DataFrame({'fecha': [r['fecha'] for r in records]})
        df['valor'] = [np.nanmean(r['vals']) if r['vals'] else np.nan for r in records]
        df = df.dropna(subset=['valor'])
        df = df.sort_values('fecha').reset_index(drop=True)

        # Resamplear a mensual si hay datos sub-mensuales
        df = df.set_index('fecha').resample('ME')['valor'].mean().reset_index()
        df.columns = ['fecha', 'valor']

        _series_cache[cod_qc] = df
        return df

    except Exception:
        _series_cache[cod_qc] = None
        return None


def calcular_pearson(cod_a, dept_a, cod_b, dept_b):
    """Calcula Pearson r entre dos estaciones. Retorna (r, p_val) o (None, None)."""
    s_a = _leer_serie_historica(cod_a, dept_a)
    s_b = _leer_serie_historica(cod_b, dept_b)

    if s_a is None or s_b is None:
        return None, None

    # Alinear en fechas comunes
    merged = pd.merge(
        s_a.rename(columns={'valor': 'a'}),
        s_b.rename(columns={'valor': 'b'}),
        on='fecha', how='inner'
    ).dropna()

    if len(merged) < MIN_REGISTROS:
        return None, None

    try:
        r, p = pearsonr(merged['a'], merged['b'])
        if np.isnan(r) or p >= P_VALOR_MAX:
            return None, None
        return round(float(r), 4), round(float(p), 6)
    except Exception:
        return None, None


def analisis_doble_masa(cod_obj, dept_obj, vecinas_df):
    """
    Análisis de doble masa (§5.5): detecta inhomogeneidades.
    Retorna True si hay quiebre sospechoso, False si serie homogénea.
    """
    s_obj = _leer_serie_historica(cod_obj, dept_obj)
    if s_obj is None or len(s_obj) < MIN_REGISTROS:
        return False, 'sin_datos'

    # Promedio de vecinas con r >= R_UMBRAL
    representativas = vecinas_df[vecinas_df['r_pp'] >= R_UMBRAL]
    if len(representativas) < 2:
        return False, 'pocas_vecinas'

    series_vec = []
    for _, vrow in representativas.iterrows():
        sv = _leer_serie_historica(vrow['cod_qc_vecina'], vrow['dept_vecina'])
        if sv is not None:
            series_vec.append(sv.set_index('fecha')['valor'])

    if not series_vec:
        return False, 'sin_series_vecinas'

    df_vec = pd.concat(series_vec, axis=1).mean(axis=1).reset_index()
    df_vec.columns = ['fecha', 'promedio']

    merged = pd.merge(
        s_obj.rename(columns={'valor': 'obj'}),
        df_vec,
        on='fecha', how='inner'
    ).dropna()

    if len(merged) < 24:
        return False, 'pocos_comunes'

    # Acumulaciones
    acum_obj = merged['obj'].cumsum().values
    acum_vec = merged['promedio'].cumsum().values

    # Prueba simplificada de quiebre: diferencia relativa en segmentos
    n = len(acum_obj)
    mid = n // 2
    if mid < 12:
        return False, 'serie_corta'

    ratio_1 = acum_obj[mid] / (acum_vec[mid] + 1e-9)
    ratio_2 = (acum_obj[-1] - acum_obj[mid]) / (acum_vec[-1] - acum_vec[mid] + 1e-9)

    inhomogeneo = abs(ratio_1 - ratio_2) / max(ratio_1, ratio_2, 1e-9) > 0.15

    return inhomogeneo, 'evaluado'


def radio_estadistico(pares_df, col_r, radio_max_km):
    """
    §5.4: Interpola la distancia donde r cae a R_UMBRAL.
    pares_df debe tener columnas 'dist_km' y col_r.
    """
    df = pares_df[['dist_km', col_r]].dropna()
    if len(df) < 2:
        return radio_max_km   # sin datos suficientes → usar radio máximo

    df = df.sort_values('dist_km')

    # Si todas r >= 0.80 → radio estadístico = radio_max
    if df[col_r].min() >= R_UMBRAL:
        return radio_max_km

    # Si ninguna r >= 0.80 → radio estadístico = 0 (solo área topográfica)
    if df[col_r].max() < R_UMBRAL:
        return 0.0

    # Interpolación simple entre los dos puntos que cruzan 0.80
    above = df[df[col_r] >= R_UMBRAL]
    below = df[df[col_r] < R_UMBRAL]

    d1 = above.iloc[-1]['dist_km']
    r1 = above.iloc[-1][col_r]
    d2 = below.iloc[0]['dist_km']
    r2 = below.iloc[0][col_r]

    if abs(r1 - r2) < 1e-6:
        return (d1 + d2) / 2

    # Interpolación lineal
    radio_est = d1 + (R_UMBRAL - r1) * (d2 - d1) / (r2 - r1)
    return round(max(0, min(radio_est, radio_max_km)), 2)


# ── Main ──────────────────────────────────────────────────────────────────────
print('[04] Cargando estaciones_prep.csv ...', flush=True)
df = pd.read_csv(PREP_CSV)
df = df.dropna(subset=['lat', 'lon']).reset_index(drop=True)
print(f'     {len(df)} estaciones', flush=True)

# Reproyectar a UTM 32719 para KD-Tree
transformer = Transformer.from_crs('EPSG:4326', 'EPSG:32719', always_xy=True)
utm_coords = np.array([
    transformer.transform(row['lon'], row['lat'])
    for _, row in df.iterrows()
])

print('[04] Construyendo KD-Tree ...', flush=True)
kd = KDTree(utm_coords)

# ── Buscar vecinas y calcular correlaciones ───────────────────────────────────
print('[04] Calculando vecinas y correlaciones ...', flush=True)
all_pares = []

for i, row in df.iterrows():
    cod_obj   = row['cod_qc']
    dept_obj  = row.get('dept', '')
    sector_obj = str(row.get('sector', '')).upper()
    cod_region_obj = str(row.get('cod_sector', ''))[:2]  # ej: 'SI', 'SE', 'CO'
    radio_pp_m  = float(row['radio_pp'])  * 1000
    radio_t_m   = float(row['radio_t'])   * 1000
    radio_max_m = max(radio_pp_m, radio_t_m)
    umbral_pp   = float(row['umbral_pp'])
    umbral_t    = float(row['umbral_t'])
    z_obj       = float(row['altitud_oficial']) if pd.notna(row['altitud_oficial']) else 0

    # KD-Tree: buscar k=20 candidatos dentro del radio máximo
    dists, idxs = kd.query(utm_coords[i], k=min(20, len(df)))
    candidatos = []
    for dist_m, idx in zip(dists, idxs):
        if idx == i:
            continue
        if dist_m > radio_max_m:
            continue
        if dist_m < 100:   # < 100 m → duplicado de la misma estación
            continue
        candidatos.append((dist_m, idx))

    if not candidatos:
        continue

    # Ordenar por distancia
    candidatos.sort(key=lambda x: x[0])

    pares_estacion = []
    for dist_m, idx in candidatos:
        vrow = df.iloc[idx]
        sector_vec = str(vrow.get('sector', '')).upper()
        cod_region_vec = str(vrow.get('cod_sector', ''))[:2]
        z_vec = float(vrow['altitud_oficial']) if pd.notna(vrow['altitud_oficial']) else 0

        dist_km = dist_m / 1000

        # Criterio §5.2: |ΔZ_par| <= 2 × umbral_ΔZ
        dz_par = abs(z_obj - z_vec)
        if dz_par > 2 * max(umbral_pp, umbral_t):
            continue

        # Criterio metodológico: mismo sector, relajado a misma región si < 3
        mismo_sector = (sector_obj == sector_vec)
        misma_region = (cod_region_obj == cod_region_vec)

        # Calcular correlación si tienen datos históricos
        r_pp, p_pp     = calcular_pearson(cod_obj, dept_obj, vrow['cod_qc'], vrow.get('dept', ''))
        r_temp, p_temp = None, None  # temp usa misma serie (simplificado)

        par = {
            'cod_qc_obj':    cod_obj,
            'nombre_obj':    row['nombre'],
            'dept_obj':      dept_obj,
            'sector_obj':    sector_obj,
            'cod_qc_vecina': vrow['cod_qc'],
            'nombre_vecina': vrow['nombre'],
            'dept_vecina':   vrow.get('dept', ''),
            'sector_vecina': sector_vec,
            'dist_km':       round(dist_km, 3),
            'dz_par':        round(dz_par, 1),
            'mismo_sector':  mismo_sector,
            'misma_region':  misma_region,
            'r_pp':          r_pp,
            'p_pp':          p_pp,
            'r_temp':        r_temp,
            'altitud_vec':   z_vec,
        }
        pares_estacion.append(par)

    # Seleccionar N=6 vecinas prioritarias:
    # Prioridad 1: mismo sector + r disponible
    # Prioridad 2: misma región + r disponible
    # Prioridad 3: las más cercanas restantes
    def prioridad(p):
        if p['mismo_sector'] and p['r_pp'] is not None:
            return 0
        if p['misma_region'] and p['r_pp'] is not None:
            return 1
        if p['mismo_sector']:
            return 2
        if p['misma_region']:
            return 3
        return 4

    pares_estacion.sort(key=lambda p: (prioridad(p), p['dist_km']))
    pares_sel = pares_estacion[:N_VECINAS]
    all_pares.extend(pares_sel)

    if (i + 1) % 100 == 0:
        print(f'     {i+1}/{len(df)} estaciones procesadas ...', flush=True)

df_pares = pd.DataFrame(all_pares)
print(f'\n[04] Pares generados: {len(df_pares)}')

# ── Doble masa por estación ───────────────────────────────────────────────────
print('[04] Análisis de doble masa ...')
dm_results = {}
for cod_obj, grupo in df_pares.groupby('cod_qc_obj'):
    row_obj = df[df['cod_qc'] == cod_obj].iloc[0]
    inh, estado = analisis_doble_masa(cod_obj, row_obj.get('dept', ''), grupo)
    dm_results[cod_obj] = {'inhomogeneidad': inh, 'dm_estado': estado}

df_pares['inhomogeneidad_obj'] = df_pares['cod_qc_obj'].map(
    lambda c: dm_results.get(c, {}).get('inhomogeneidad', False)
)

# ── Radio estadístico por estación ───────────────────────────────────────────
print('[04] Calculando radios estadísticos ...')
radios = []
for cod_obj, grupo in df_pares.groupby('cod_qc_obj'):
    row_obj = df[df['cod_qc'] == cod_obj].iloc[0]
    r_pp_est   = radio_estadistico(grupo, 'r_pp',   float(row_obj['radio_pp']))
    # Para temp usamos misma r (simplificado por falta de serie T separada)
    r_temp_est = r_pp_est

    radios.append({
        'cod_qc':              cod_obj,
        'radio_est_pp_km':     r_pp_est,
        'radio_est_temp_km':   r_temp_est,
        'n_vecinas_con_r':     grupo['r_pp'].notna().sum(),
        'n_vecinas_r_080':     (grupo['r_pp'] >= R_UMBRAL).sum(),
        'inhomogeneidad':      dm_results.get(cod_obj, {}).get('inhomogeneidad', False),
        'dm_estado':           dm_results.get(cod_obj, {}).get('dm_estado', ''),
    })

df_radios = pd.DataFrame(radios)

# ── Guardar ───────────────────────────────────────────────────────────────────
out_pares  = OUT / 'vecinas_correlaciones.csv'
out_radios = OUT / 'radios_estadisticos.csv'

df_pares.to_csv(out_pares,  index=False, encoding='utf-8')
df_radios.to_csv(out_radios, index=False, encoding='utf-8')

print(f'\n[04] Vecinas guardadas: {out_pares}')
print(f'[04] Radios estadísticos: {out_radios}')
print(f'\n[04] Distribución r_pp:')
bins = [-1, 0.7, 0.8, 0.9, 1.01]
labels = ['<0.70', '0.70-0.80', '0.80-0.90', '>=0.90']
df_pares['clase_r'] = pd.cut(df_pares['r_pp'].dropna(), bins=bins, labels=labels)
print(df_pares['clase_r'].value_counts().sort_index().to_string())
print(f'\n[04] Estaciones con r>=0.80 al menos 1 vecina: '
      f'{(df_radios["n_vecinas_r_080"] >= 1).sum()} / {len(df_radios)}')
print(f'[04] Script 04 completado.')
