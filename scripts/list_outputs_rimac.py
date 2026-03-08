from pathlib import Path
import sys
base = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(base))

try:
    import app as scraper
    import server as srv
except Exception as e:
    print('Error importing project modules:', e)
    raise

import pandas as pd
from shapely.geometry import shape, Point

outputs = base / 'DATA' / 'outputs'
shp = base / 'DATA' / 'CUENCAS' / 'UH.shp'

if not outputs.exists():
    print('No outputs folder')
    sys.exit(0)

# build rimac polygons
geo = srv.shapefile_to_geojson(shp)
features = geo.get('features', [])
polys = [shape(f['geometry']) for f in features if f.get('geometry')]

files = [p for p in sorted(outputs.glob('*.xlsx'), key=lambda x: x.stat().st_mtime, reverse=True)]
results = []

# load station master
df_st = scraper.get_stations(use_local=True)

print('Files in outputs:', len(files))
print('Stations in master:', len(df_st))
for p in files:
    name = p.stem
    print('Checking file:', p.name)
    # try to find station name in master by substring
    found_row = None
    for idx, r in df_st.iterrows():
        try:
            if r.get('estacion') and r.get('estacion').strip().lower() in name.lower():
                found_row = r
                break
        except Exception:
            continue
    if found_row is None or (hasattr(found_row, 'empty') and found_row.empty):
        # try by code in filename
        for idx, r in df_st.iterrows():
            try:
                if r.get('cod') and str(r.get('cod')) in name:
                    found_row = r
                    break
            except Exception:
                continue
    if found_row is None or (hasattr(found_row, 'empty') and found_row.empty):
        continue
    lat = None; lon = None
    try:
        lat = float(found_row.get('lat'))
        lon = float(found_row.get('lon'))
    except Exception:
        continue
    pt = Point(lon, lat)
    inside = any((p.contains(pt) or p.intersects(pt)) for p in polys)
    if not inside:
        continue
    # read excel to get date range
    try:
        df = pd.read_excel(p)
        cols = [str(c) for c in df.columns]
        date_col = None
        for c in cols:
            lc = c.lower()
            if 'fecha' in lc or 'date' in lc or 'time' in lc:
                date_col = c; break
        if date_col is None:
            for c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]):
                    date_col = c; break
        dmin = dmax = None
        if date_col is not None:
            s = pd.to_datetime(df[date_col], errors='coerce')
            if s.notna().any():
                dmin = str(s.min().date())
                dmax = str(s.max().date())
        results.append({'file': p.name, 'station': found_row.get('estacion'), 'cod': found_row.get('cod'), 'lat': lat, 'lon': lon, 'date_min': dmin, 'date_max': dmax, 'path': str(p)})
    except Exception as e:
        results.append({'file': p.name, 'station': found_row.get('estacion'), 'cod': found_row.get('cod'), 'lat': lat, 'lon': lon, 'date_min': None, 'date_max': None, 'path': str(p)})

# show up to 6
for r in results[:6]:
    print(r)

if not results:
    print('No outputs encontrados dentro de Rímac')

# save summary
out = outputs / 'rimac_outputs_summary.json'
import json
with open(out, 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)
print('Resumen guardado en', out)
