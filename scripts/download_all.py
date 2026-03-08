import argparse
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import get_stations, process_all_stations


def main():
    p = argparse.ArgumentParser(description='Descargar data histórica para todas las estaciones')
    p.add_argument('--from', dest='from_date', default='2015-06-11')
    p.add_argument('--to', dest='to_date', default='2026-02-21')
    p.add_argument('--use-local', dest='use_local', action='store_true', default=True)
    p.add_argument('--no-local', dest='use_local', action='store_false')
    p.add_argument('--limit', dest='limit', type=int, default=None)
    args = p.parse_args()

    print('Cargando estaciones (use_local=%s)...' % args.use_local)
    df = get_stations(use_local=args.use_local)
    print('Estaciones encontradas:', len(df))
    print('Procesando desde', args.from_date, 'hasta', args.to_date, 'limit=', args.limit)

    saved = process_all_stations(df, from_date=args.from_date, to_date=args.to_date, limit=args.limit, verbose=True)

    print('\nResumen:')
    print('Archivos guardados:', len(saved))
    if saved:
        for s in saved:
            print(' -', s)


if __name__ == '__main__':
    main()
