from pathlib import Path
from datetime import datetime
import traceback
import sys

# Asegurar que la carpeta del proyecto esté en sys.path para importar `app.py`
base_candidate = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(base_candidate))

try:
    import app as scraper
except Exception as e:
    print('No se puede importar `app.py`:', e)
    raise


def main():
    base = Path(__file__).resolve().parents[1]
    data_dir = base / 'DATA'
    shp = data_dir / 'CUENCAS' / 'UH.shp'

    if not shp.exists():
        print('Shapefile no encontrado:', shp)
        return

    print('Cargando estaciones (local o remota si hace falta)...')
    df = scraper.get_stations(use_local=True)

    print('Filtrando estaciones dentro de la cuenca Rímac (usando server.shapefile_to_geojson)...')
    try:
        import server as srv
        from shapely.geometry import shape, Point

        geo = srv.shapefile_to_geojson(shp)
        features = geo.get('features', [])
        polys = [shape(f['geometry']) for f in features if f.get('geometry')]

        if not polys:
            print('No se encontraron polígonos en el shapefile')
            return

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

        stations_inside = df.loc[inside_idx].copy()
    except Exception as e:
        print('Error filtrando por shapefile:', e)
        traceback.print_exc()
        return

    n = len(stations_inside)
    print(f'→ {n} estaciones encontradas dentro de la cuenca')
    if n == 0:
        return

    from_date = '2015-01-01'
    to_date = datetime.now().strftime('%Y-%m-%d')

    outputs = data_dir / 'outputs'
    outputs.mkdir(parents=True, exist_ok=True)

    saved = []

    # Usar df original para índices correctos en save_station_by_index
    df_for_search = df.reset_index(drop=True)

    for i, row in stations_inside.reset_index(drop=True).iterrows():
        station_name = row.get('estacion') or row.get('NOMBRE_ESTACION') or row.get('nombre') or None
        print('\n===========================================')
        print(f'[{i+1}/{n}] Procesando: {station_name}')
        try:
            path = scraper.save_station_by_name(df_for_search, station_name, from_date, to_date, output_dir=outputs)
            print('Guardado en:', path)
            if path:
                saved.append(str(path))
        except Exception as e:
            print('Error descargando estación:', e)
            traceback.print_exc()

    print('\nResumen:')
    print('Total estaciones procesadas:', n)
    print('Archivos guardados:', len(saved))

    list_file = outputs / 'rimac_saved_list.txt'
    try:
        with open(list_file, 'w', encoding='utf-8') as f:
            for p in saved:
                f.write(p + '\n')
        print('Lista de archivos guardados en:', list_file)
    except Exception as e:
        print('No se pudo escribir la lista de guardados:', e)


if __name__ == '__main__':
    main()
