import sys
from pathlib import Path
from datetime import datetime, timedelta

# asegurar que el root del proyecto esté en sys.path para importar `app`
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import app as scraper


def main():
    to_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    from_date = '2015-01-01'
    station_name = 'ACOBAMBA'

    print(f"Cargando estaciones (use_local=True)...")
    df = scraper.get_stations(use_local=True)
    print(f"Estaciones cargadas: {len(df)}")

    print(f"Iniciando guardado de {station_name} ({from_date} → {to_date})...")
    out = scraper.save_station_by_name(df, station_name, from_date, to_date)
    print('Ruta devuelta:', out)

    if out:
        p = Path(out)
        print('Existe:', p.exists())
        try:
            print('Tamaño (bytes):', p.stat().st_size)
        except Exception as e:
            print('No se pudo obtener tamaño:', e)

if __name__ == '__main__':
    main()
