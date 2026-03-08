import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import get_stations, save_station_by_name, get_station_data

def main():
    print('Cargando lista de estaciones (local)...')
    df = get_stations(use_local=True)
    print('Estaciones cargadas:', len(df))
    mask = df['estacion'].str.contains('SANTA ANITA', case=False, na=False)
    found = df[mask]
    print('Coincidencias locales:', len(found))
    if len(found):
        print(found[['estacion','cod','ico','cod_old','lat','lon']].to_string(index=False))
    else:
        print('No encontrada localmente. Intentando descarga remota de estaciones...')
        df2 = get_stations(use_local=False)
        mask2 = df2['estacion'].str.contains('SANTA ANITA', case=False, na=False)
        found2 = df2[mask2]
        print('Coincidencias remotas:', len(found2))
        if len(found2):
            print(found2[['estacion','cod','ico','cod_old','lat','lon']].to_string(index=False))
            found = found2

    if len(found):
        print('\nIntentando guardar datos para SANTA ANITA (2024-01-01 -> 2024-02-01) ...')
        # pass the filtered df to save_station_by_name
        path = save_station_by_name(found.reset_index(drop=True), 'SANTA ANITA', '2024-01-01', '2024-02-01')
        print('Resultado guardado:', path)
        print('\nIntentando obtener datos en memoria (get_station_data) ...')
        data = get_station_data('SANTA ANITA', '2024-01-01', '2024-02-01', df_stations=found.reset_index(drop=True))
        if data is None:
            print('No se descargaron datos.')
        else:
            print('Registros descargados:', len(data))
            print('Columnas:', list(data.columns))
    else:
        print('No se encontró ninguna estación llamada SANTA ANITA.')

if __name__ == '__main__':
    main()
