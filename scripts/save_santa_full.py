from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import get_stations, save_station_by_name, get_station_data


def main():
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description='Guardar historial completo de una estación (usa funciones de app.py)')
    parser.add_argument('-s', '--station', required=True, help='Nombre (o parte) de la estación a procesar')
    parser.add_argument('-f', '--from-date', default='2015-06-11', help='Fecha inicio YYYY-MM-DD')
    parser.add_argument('-t', '--to-date', default=datetime.now().strftime('%Y-%m-%d'), help='Fecha fin YYYY-MM-DD')
    parser.add_argument('--no-local', dest='use_local', action='store_false', help='No usar archivo local; forzar descarga remota')
    parser.add_argument('--output-dir', default=None, help='Directorio donde guardar salidas (opcional)')

    args = parser.parse_args()

    station_name = args.station
    from_date = args.from_date
    to_date = args.to_date
    use_local = args.use_local if 'use_local' in args else True

    print(f"Cargando estaciones (use_local={use_local})...")
    df = get_stations(use_local=use_local)

    mask = df['estacion'].str.contains(station_name, case=False, na=False)
    found = df[mask]
    if len(found) == 0 and use_local:
        print('No encontrada localmente; obteniendo lista remota...')
        df = get_stations(use_local=False)
        mask = df['estacion'].str.contains(station_name, case=False, na=False)
        found = df[mask]

    if len(found) == 0:
        print(f'✗ No se encontró "{station_name}" en las listas de estaciones.')
        return

    print('Estación(es) encontrada(s):')
    print(found[['estacion', 'cod', 'ico', 'cod_old', 'lat', 'lon']].to_string(index=False))

    print(f"Descargando historial completo {from_date} -> {to_date} y guardando...")
    out = save_station_by_name(df.reset_index(drop=True), station_name, from_date, to_date, output_dir=args.output_dir)
    print('Resultado guardado en:', out)

    if out:
        print('Comprobando contenido en memoria...')
        data = get_station_data(station_name, from_date, to_date, df_stations=df.reset_index(drop=True))
        if data is None:
            print('No se descargaron registros en memoria.')
        else:
            print('Registros descargados en memoria:', len(data))


if __name__ == '__main__':
    main()
