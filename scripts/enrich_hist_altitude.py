"""
enrich_hist_altitude.py
=======================
Enriquece DATA/stations_hist_portal.csv con la columna 'altitud'.

Estrategia por prioridad:
  1. Maestra_de_estaciones_Senamhi.xlsx  → cruce por nombre exacto (normalizado)
  2. Estaciones_Meteorologicas_Peru.xlsx → cruce por cod_ho
  3. OpenTopoData API (SRTM 90m)         → elevación desde coordenadas lat/lon
     para las estaciones que siguen sin altitud

Salida: DATA/stations_hist_portal.csv (sobreescrito con columna 'altitud' añadida)
        scripts/enrich_report.txt         (resumen detallado)

Uso:
    .venv/Scripts/python.exe scripts/enrich_hist_altitude.py
"""

import pandas as pd
import glob
import time
import requests
import unicodedata
import re
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
DATA = ROOT / 'DATA'

# ── helpers ──────────────────────────────────────────────────────────────────

def norm(s):
    """Normaliza string: mayúsculas, sin acentos, sin chars raros."""
    s = str(s or '').upper().strip()
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = re.sub(r'\s+', ' ', s)
    return s

def fetch_elevations_opentopodata(coords, dataset='srtm90m', batch=100):
    """
    Consulta OpenTopoData API para una lista de (lat, lon).
    Retorna lista de elevaciones en el mismo orden (None si falla).
    https://www.opentopodata.org/
    """
    results = [None] * len(coords)
    for i in range(0, len(coords), batch):
        chunk = coords[i:i+batch]
        locations = '|'.join(f"{lat},{lon}" for lat, lon in chunk)
        url = f'https://api.opentopodata.org/v1/{dataset}?locations={locations}'
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if data.get('status') == 'OK':
                    for j, result in enumerate(data['results']):
                        elev = result.get('elevation')
                        if elev is not None:
                            results[i + j] = round(elev)
            else:
                print(f"  OpenTopoData HTTP {r.status_code}: {r.text[:200]}")
        except Exception as e:
            print(f"  OpenTopoData error: {e}")
        if i + batch < len(coords):
            time.sleep(1.2)  # respetar rate-limit (~1 req/seg)
    return results

# ── cargar CSV histórico ──────────────────────────────────────────────────────

csv_path = DATA / 'stations_hist_portal.csv'
if not csv_path.exists():
    sys.exit(f"ERROR: No se encontró {csv_path}")

df = pd.read_csv(csv_path)
print(f"CSV histórico: {len(df)} estaciones")
print(f"Columnas: {df.columns.tolist()}")

# Asegurarnos de que 'altitud' no está ya (o limpiarla si existe)
if 'altitud' in df.columns:
    print(f"  → 'altitud' ya existe: {df.altitud.notna().sum()} con dato")
    df['altitud'] = pd.to_numeric(df['altitud'], errors='coerce')
else:
    df['altitud'] = None

df['nombre_norm'] = df['estacion'].apply(norm)
df['cod_ho_s']    = df['cod_ho'].astype(str).str.strip()

report = []

# ── FUENTE 1: Maestra_de_estaciones_Senamhi.xlsx ─────────────────────────────

maestra_files = list(DATA.glob('Maestra*.xlsx'))
if not maestra_files:
    print("AVISO: No se encontró Maestra_de_estaciones_Senamhi.xlsx")
    df_m = pd.DataFrame(columns=['nombre_norm', 'ALTITUD'])
else:
    df_m = pd.read_excel(maestra_files[0])
    df_m['nombre_norm'] = df_m['NOMBRE_ESTACION'].apply(norm)
    # quedarnos con la altitud más común si hay duplicados de nombre
    df_m_dedup = df_m.dropna(subset=['ALTITUD']).drop_duplicates('nombre_norm', keep='first')
    df_m_map = dict(zip(df_m_dedup['nombre_norm'], df_m_dedup['ALTITUD']))
    print(f"Maestra: {len(df_m)} filas, {len(df_m_map)} nombres únicos con altitud")

    n_before = df['altitud'].notna().sum()
    mask_falta = df['altitud'].isna()
    df.loc[mask_falta, 'altitud'] = df.loc[mask_falta, 'nombre_norm'].map(df_m_map)
    n_after = df['altitud'].notna().sum()
    gained = n_after - n_before
    print(f"  → Fuente 1 (Maestra nombre): +{gained} estaciones ({n_after}/{len(df)} totales)")
    report.append(f"Fuente 1 (Maestra nombre exacto): +{gained} ({n_after}/{len(df)})")

# ── FUENTE 2: Estaciones_Meteorologicas_Peru.xlsx (DMS) ───────────────────────

dms_files = list(DATA.glob('Estaciones*.xlsx')) + list(DATA.glob('estaciones*.xlsx'))
if not dms_files:
    print("AVISO: No se encontró Estaciones_Meteorologicas_Peru.xlsx")
else:
    df_dms = pd.read_excel(dms_files[0], header=None)
    # col 0 = cod_ho, col 7 = altitud
    df_dms.columns = ['cod_ho_dms','lat_g','lat_m','lat_s','lon_g','lon_m','lon_s','altitud_dms','nombre_dms'] + list(range(9, len(df_dms.columns)))
    df_dms['cod_ho_dms'] = df_dms['cod_ho_dms'].astype(str).str.strip()
    dms_map = dict(zip(df_dms['cod_ho_dms'], df_dms['altitud_dms']))

    n_before = df['altitud'].notna().sum()
    mask_falta = df['altitud'].isna()
    df.loc[mask_falta, 'altitud'] = df.loc[mask_falta, 'cod_ho_s'].map(dms_map)
    n_after = df['altitud'].notna().sum()
    gained = n_after - n_before
    print(f"  → Fuente 2 (Excel DMS cod_ho): +{gained} estaciones ({n_after}/{len(df)} totales)")
    report.append(f"Fuente 2 (Excel DMS cod_ho): +{gained} ({n_after}/{len(df)})")

# ── FUENTE 3: OpenTopoData (SRTM90m) para las restantes ──────────────────────

sin_alt = df[df['altitud'].isna() & df['lat'].notna() & df['lon'].notna()]
print(f"\nSin altitud todavía: {len(sin_alt)} estaciones")
if len(sin_alt) > 0:
    print("  Consultando OpenTopoData (SRTM 90m)...")
    coords = list(zip(sin_alt['lat'], sin_alt['lon']))
    elevs  = fetch_elevations_opentopodata(coords)

    idx_list = sin_alt.index.tolist()
    ok = 0
    for i, (idx, elev) in enumerate(zip(idx_list, elevs)):
        if elev is not None:
            df.at[idx, 'altitud'] = elev
            ok += 1
    n_after = df['altitud'].notna().sum()
    print(f"  → Fuente 3 (OpenTopoData SRTM): +{ok} estaciones ({n_after}/{len(df)} totales)")
    report.append(f"Fuente 3 (OpenTopoData SRTM 90m): +{ok} ({n_after}/{len(df)})")
else:
    print("  ✓ Todas tienen altitud — no se necesita API externa")

# ── Limpiar y convertir a entero ──────────────────────────────────────────────

df['altitud'] = pd.to_numeric(df['altitud'], errors='coerce').round(0)
# Convertir a Int64 (permite NaN) para evitar .0 en el CSV
df['altitud'] = df['altitud'].astype('Int64')

# ── Guardar CSV enriquecido ───────────────────────────────────────────────────

# Eliminar columnas auxiliares temporales
df = df.drop(columns=['nombre_norm', 'cod_ho_s'], errors='ignore')

# Reordenar para que altitud quede junto a las coords
cols = ['estacion', 'cod', 'cod_qc', 'cod_ho', 'lat', 'lon', 'altitud', 'departamento']
extras = [c for c in df.columns if c not in cols]
df = df[cols + extras]

df.to_csv(csv_path, index=False)
print(f"\n✅ CSV guardado: {csv_path}")
print(f"   Total estaciones: {len(df)}")
print(f"   Con altitud: {df.altitud.notna().sum()}")
print(f"   Sin altitud: {df.altitud.isna().sum()}")

# ── Reporte detallado ─────────────────────────────────────────────────────────

report_path = Path(__file__).parent / 'enrich_report.txt'
lines = ['=== REPORTE enrich_hist_altitude.py ===\n']
lines += [r + '\n' for r in report]
lines.append(f'\nTotal final: {df.altitud.notna().sum()}/{len(df)} con altitud\n')

sin_final = df[df.altitud.isna()][['estacion','cod','departamento']]
if len(sin_final):
    lines.append(f'\nEstaciones SIN altitud ({len(sin_final)}):\n')
    for _, r in sin_final.iterrows():
        lines.append(f"  [{r['cod']}] {r['estacion']} ({r['departamento']})\n")
else:
    lines.append('\n✓ Todas las estaciones tienen altitud.\n')

with open(report_path, 'w', encoding='utf-8') as f:
    f.writelines(lines)
print(f"\n📄 Reporte guardado: {report_path}")
if len(sin_final):
    print(f"   {len(sin_final)} estaciones sin altitud (ver reporte)")
