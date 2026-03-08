import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import save_station_by_name, get_station_data, get_stations

if __name__ == '__main__':
    station = 'ALAMOR'
    from_date = '2024-01-01'
    to_date = '2024-01-31'
    df = get_stations(use_local=True)
    print(f"Intentando guardar {station} {from_date} -> {to_date}")
    out = save_station_by_name(df.reset_index(drop=True), station, from_date, to_date, output_dir=None)
    print('save_station_by_name ->', out)
    data = get_station_data(station, from_date, to_date, df_stations=df.reset_index(drop=True))
    print('get_station_data ->', None if data is None else f"{len(data)} registros")
