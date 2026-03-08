from pathlib import Path
import sys
from datetime import datetime
base = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(base))

try:
    import app as scraper
except Exception as e:
    print('Error importing app:', e)
    raise

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_link(cod, ico, estado, cod_old, yyyymm):
    if pd.isna(cod_old) if 'pd' in globals() else (cod_old is None):
        return f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={yyyymm}&t_e={ico}&estado={estado}"
    else:
        return f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={yyyymm}&t_e={ico}&estado={estado}&cod_old={cod_old}"


if __name__ == '__main__':
    import pandas as pd
    from shapely.geometry import shape, Point
    import server as srv

    df = scraper.get_stations(use_local=True)
    shp = base / 'DATA' / 'CUENCAS' / 'UH.shp'
    geo = srv.shapefile_to_geojson(shp)
    features = geo.get('features', [])
    polys = [shape(f['geometry']) for f in features if f.get('geometry')]

    rimac = []
    for idx, r in df.iterrows():
        try:
            lat = float(r.get('lat'))
            lon = float(r.get('lon'))
        except Exception:
            continue
        pt = Point(lon, lat)
        if any((p.contains(pt) or p.intersects(pt)) for p in polys):
            rimac.append((idx, r))

    print('Rímac stations found:', len(rimac))

    session = requests.Session()
    retries = Retry(total=2, backoff_factor=0.2, status_forcelist=[429,500,502,503,504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    years = list(range(2010, datetime.now().year+1))
    results = []
    max_check = 200
    checked = 0
    for idx, r in rimac:
        if checked >= max_check:
            break
        checked += 1
            if checked % 20 == 0:
                print('Checked', checked)
        cod = r.get('cod')
        ico = r.get('ico') or ''
        estado = r.get('estado') or ''
        cod_old = r.get('cod_old') if 'cod_old' in r.index else None
        years_with = 0
        first_year = None
        for y in years:
            yyyymm = f"{y}01"
            try:
                if pd.isna(cod):
                    continue
            except Exception:
                pass
            link = build_link(cod, ico, estado, cod_old, yyyymm)
            try:
                resp = session.get(link, timeout=10)
                if resp.status_code == 200:
                    import pandas as _pd
                    try:
                        tables = _pd.read_html(resp.text)
                        if len(tables) > 1:
                            years_with += 1
                            if first_year is None:
                                first_year = y
                    except Exception:
                        pass
            except Exception:
                pass
        print('Station', checked, r.get('estacion'), 'years_with_data=', years_with, 'first_year=', first_year)
        results.append({'idx': idx, 'station': r.get('estacion'), 'cod': cod, 'years_with_data': years_with, 'first_year': first_year})

    # sort by years_with_data desc then first_year asc
    results = sorted(results, key=lambda x: (-x['years_with_data'], x['first_year'] or 9999))
    top6 = results[:6]
    import json
    out = base / 'DATA' / 'outputs' / 'rimac_sampled_top6.json'
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(top6, f, ensure_ascii=False, indent=2)
    print('Top6 (saved):')
    for t in top6:
        print(t)
    print('Saved to', out)
