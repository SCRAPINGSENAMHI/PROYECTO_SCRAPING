import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import save_each_station_verbose

if __name__ == '__main__':
    print('Iniciando prueba limit=1')
    res = save_each_station_verbose(from_date='2015-06-11', to_date='2015-06-11', use_local=True, output_dir=None, limit=1, verbose=True)
    print('\nSAVED:', res)
