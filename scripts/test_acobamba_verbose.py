import sys
from pathlib import Path
from datetime import datetime, timedelta
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import app

name = 'ACOBAMBA'
from_date = '2025-01-01'
to_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

print('Cargando lista remota...')
df = app.get_stations(use_local=False)
row = df[df['estacion'].astype(str).str.contains(name, case=False, na=False)]
if len(row)==0:
    print('No encontrado en remota')
    sys.exit(1)

row = row.iloc[0]
print('Params:', dict(estacion=row['estacion'], cod=row['cod'], cod_old=row.get('cod_old'), ico=row.get('ico'), estado=row.get('estado')))

print('\nIniciando descarga VERBOSE (rango reducido):')
dfres = app.download_station_data(row.get('cod'), row.get('ico'), row.get('estado'), row.get('cod_old'), from_date, to_date, verbose=True)
if dfres is None:
    print('\nNo se encontraron tablas en el rango de prueba')
else:
    print('\nRegistros descargados:', len(dfres))
    print(dfres.head().to_string(index=False))

print('\nDone')
