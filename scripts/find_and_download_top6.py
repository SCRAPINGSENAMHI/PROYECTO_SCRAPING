from pathlib import Path
import sys
from datetime import datetime

base = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(base))

try:
    import app as scraper
    import server as srv
except Exception as e:
    print('Error importing project modules:', e)
    raise

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from shapely.geometry import shape, Point
import time
import json

OUT_DIR = base / 'DATA' / 'outputs'
OUT_DIR.mkdir(parents=True, exist_ok=True)

SHAPE = base / 'DATA' / 'CUENCAS' / 'UH.shp'
if not SHAPE.exists():
    print('Shapefile not found:', SHAPE)
    sys.exit(1)

# Build Rímac polygons
geo = srv.shapefile_to_geojson(SHAPE)
features = geo.get('features', [])
polys = [shape(f['geometry']) for f in features if f.get('geometry')]

# Stations
df = scraper.get_stations(use_local=True)
rimac_idx = []
for idx, r in df.iterrows():
    try:
        lat = float(r.get('lat'))
        lon = float(r.get('lon'))
    except Exception:
        continue
    pt = Point(lon, lat)
    if any((p.contains(pt) or p.intersects(pt)) for p in polys):
        rimac_idx.append(idx)

print('Stations in Rímac:', len(rimac_idx))

# HTTP session
session = requests.Session()
retries = Retry(total=2, backoff_factor=0.3, status_forcelist=[429,500,502,503,504])
session.mount('https://', HTTPAdapter(max_retries=retries))
session.mount('http://', HTTPAdapter(max_retries=retries))

start_year = 2015
end_year = datetime.now().year
years = list(range(start_year, end_year+1))

candidates = []
max_check = 200
checked = 0

for idx in rimac_idx:
    if checked >= max_check or len(candidates) >= 6:
        break
    checked += 1
    row = df.loc[idx]
    cod = row.get('cod') or ''
    ico = row.get('ico') or ''
    estado = row.get('estado') or ''
    cod_old = row.get('cod_old') if 'cod_old' in row.index else None
    station_name = row.get('estacion')
    if not cod:
        continue
    years_with = 0
    first_year = None
    print(f"[{checked}] Checking {station_name} (cod={cod})")
    for y in years:
        yyyymm = f"{y}01"
        if cod_old and str(cod_old).strip() != 'None':
            link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={yyyymm}&t_e={ico}&estado={estado}&cod_old={cod_old}"
        else:
            link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={yyyymm}&t_e={ico}&estado={estado}"
        try:
            resp = session.get(link, timeout=10)
            if resp.status_code == 200:
                try:
                    tables = pd.read_html(resp.text)
                    if len(tables) > 1:
                        years_with += 1
                        if first_year is None:
                            first_year = y
                except Exception:
                    pass
        except Exception:
            pass
        # polite pause
        time.sleep(0.05)
    print('  years_with:', years_with, 'first_year:', first_year)
    if years_with == len(years):
        candidates.append({'idx': int(idx), 'station': station_name, 'cod': cod, 'ico': ico, 'estado': estado, 'cod_old': cod_old})

# If not enough strict complete stations, relax to those with >= 80% years
if len(candidates) < 6:
    print('No suficientes con coverage 100% — relajando criterio a >=80% de años')
    threshold = int(len(years) * 0.8)
    checked = 0
    for idx in rimac_idx:
        if checked >= max_check or len(candidates) >= 6:
            break
        checked += 1
        row = df.loc[idx]
        cod = row.get('cod') or ''
        ico = row.get('ico') or ''
        estado = row.get('estado') or ''
        cod_old = row.get('cod_old') if 'cod_old' in row.index else None
        station_name = row.get('estacion')
        if not cod:
            continue
        years_with = 0
        for y in years:
            yyyymm = f"{y}01"
            if cod_old and str(cod_old).strip() != 'None':
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={yyyymm}&t_e={ico}&estado={estado}&cod_old={cod_old}"
            else:
                link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={yyyymm}&t_e={ico}&estado={estado}"
            try:
                resp = session.get(link, timeout=8)
                if resp.status_code == 200:
                    try:
                        tables = pd.read_html(resp.text)
                        if len(tables) > 1:
                            years_with += 1
                    except Exception:
                        pass
            except Exception:
                pass
            time.sleep(0.03)
        if years_with >= threshold:
            print('  candidate relaxed:', station_name, 'years_with', years_with)
            candidates.append({'idx': int(idx), 'station': station_name, 'cod': cod, 'ico': ico, 'estado': estado, 'cod_old': cod_old})
        # avoid duplicates
        candidates = [dict(t) for t in {tuple(d.items()) for d in candidates}]

print('\nCandidates found:', len(candidates))
for c in candidates[:6]:
    print('-', c['station'], c['cod'])

# Download full data for up to 6 candidates
saved = []
for c in candidates[:6]:
    try:
        print('Downloading full for', c['station'])
        path = scraper.save_station_by_name(df.reset_index(drop=True), c['station'], from_date=f"{start_year}-01-01", to_date=datetime.now().strftime('%Y-%m-%d'), output_dir=OUT_DIR)
        print('  saved:', path)
        saved.append({'station': c['station'], 'cod': c['cod'], 'path': str(path) if path else None})
    except Exception as e:
        print('  error saving', c['station'], e)

outf = OUT_DIR / 'rimac_top6_downloaded.json'
with open(outf, 'w', encoding='utf-8') as f:
    json.dump({'candidates': candidates[:6], 'saved': saved}, f, ensure_ascii=False, indent=2)
print('Result saved to', outf)
