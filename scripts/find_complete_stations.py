from pathlib import Path
from datetime import datetime
import sys

# preparar path al proyecto
base_candidate = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(base_candidate))

try:
    import app as scraper
    import server as srv
except Exception as e:
    print('Error importing project modules:', e)
    raise

from shapely.geometry import shape, Point
import pandas as pd


def filter_rimac(df):
    shp = Path(__file__).resolve().parents[1] / 'DATA' / 'CUENCAS' / 'UH.shp'
    geo = srv.shapefile_to_geojson(shp)
    features = geo.get('features', [])
    polys = [shape(f['geometry']) for f in features if f.get('geometry')]
    inside_idx = []
    for idx, r in df.iterrows():
        try:
            lat = float(r.get('lat')) if r.get('lat') not in (None, '') else None
            lon = float(r.get('lon')) if r.get('lon') not in (None, '') else None
        except Exception:
            lat = lon = None
        if lat is None or lon is None:
            continue
        pt = Point(lon, lat)
        for p in polys:
            try:
                if p.contains(pt) or p.intersects(pt):
                    inside_idx.append(idx)
                    break
            except Exception:
                continue
    return df.loc[inside_idx].copy()


def detect_date_column(df):
    cols = [str(c) for c in df.columns]
    candidates = [c for c in cols if any(k in c.lower() for k in ('fecha','date','año','ano','mes','dia','time'))]
    if candidates:
        return candidates[0]
    # fallback: first column
    return cols[0] if cols else None


def has_full_range(df, start='2013-01-01', end=None):
    if end is None:
        end = datetime.now().strftime('%Y-%m-%d')
    date_col = detect_date_column(df)
    if date_col is None:
        return False, None, None
    try:
        s = pd.to_datetime(df[date_col], errors='coerce')
        if s.notna().any():
            dmin = s.min().date()
            dmax = s.max().date()
            ok = (dmin <= pd.to_datetime(start).date()) and (dmax >= pd.to_datetime(end).date())
            return bool(ok), str(dmin), str(dmax)
    except Exception:
        return False, None, None
    return False, None, None


def main():
    df = scraper.get_stations(use_local=True)
    rimac = filter_rimac(df)
    print('Estaciones en Rímac:', len(rimac))

    df_for_search = df.reset_index(drop=True)
    found = []
    start = '2015-01-01'
    end = datetime.now().strftime('%Y-%m-%d')
    max_trials = 200
    trials = 0

    for idx, row in rimac.reset_index(drop=True).iterrows():
        if trials >= max_trials or len(found) >= 6:
            break
        station_name = row.get('estacion') or row.get('NOMBRE_ESTACION') or row.get('nombre') or None
        print(f"\n[{trials+1}] Probando estación: {station_name}")
        try:
            data = scraper.get_station_data(station_name, from_date=start, to_date=end, df_stations=df_for_search)
            trials += 1
            if data is None:
                continue
            ok, dmin, dmax = has_full_range(data, start=start, end=end)
            print('  rango detectado:', dmin, '->', dmax, 'completo?' , ok)
            if ok:
                found.append({'station': station_name, 'index': idx, 'date_min': dmin, 'date_max': dmax})
        except Exception as e:
            print('  error:', e)
            trials += 1
            continue

    print('\nEncontradas:', len(found))
    for f in found:
        print('-', f)

    out = Path(__file__).resolve().parents[1] / 'DATA' / 'outputs' / 'rimac_complete_6.json'
    try:
        import json
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, 'w', encoding='utf-8') as fp:
            json.dump(found, fp, ensure_ascii=False, indent=2)
        print('Guardado en:', out)
    except Exception as e:
        print('No se pudo guardar resultados:', e)


if __name__ == '__main__':
    main()
