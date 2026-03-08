import sys
from pathlib import Path
from datetime import datetime, timedelta
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import app

name = 'ACOBAMBA'
from_date = '2015-01-01'
to_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

print('Cargando lista remota de estaciones...')
df = app.get_stations(use_local=False)
print('Stations loaded:', len(df))

print('Guardando historial completo para', name, f'({from_date} → {to_date})')
out = app.save_station_by_name(df, name, from_date, to_date)
print('Resultado:', out)
if out:
    p = Path(out)
    print('Existe:', p.exists(), 'Tam(bytes):', p.stat().st_size)
else:
    print('No se generó archivo.')
