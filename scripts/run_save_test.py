import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from datetime import datetime
from app import save_each_station_verbose

if __name__ == '__main__':
    from_date = '2020-01-01'
    to_date = '2015-06-11'
    limit = 3

    # Corregir orden si es necesario
    if datetime.fromisoformat(from_date) > datetime.fromisoformat(to_date):
        from_date, to_date = to_date, from_date

    print('Ejecutando prueba con:', from_date, to_date, 'limit=', limit)
    res = save_each_station_verbose(from_date=from_date, to_date=to_date, use_local=True, output_dir=None, limit=limit, verbose=True)
    print('\nSAVED:', res)
