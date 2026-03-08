from pathlib import Path
from datetime import datetime
import json
import sys

base = Path(__file__).resolve().parents[1]
OUT = base / 'DATA' / 'outputs'
OUT.mkdir(parents=True, exist_ok=True)

stations = [
    'LA OROYA',
    'RICRAN',
    'AUTISHA',
    'VON HUMBOLDT',
    'CAMPO DE MARTE',
    'ÑAÑA'
]

from_date = '2015-01-01'
to_date = datetime.now().strftime('%Y-%m-%d')

summary = []

# ensure project base on path
sys.path.insert(0, str(base))
import app

print('Obteniendo lista remota de estaciones...')
df = app.get_stations(use_local=False)

for name in stations:
    print('\n==============================')
    print('Procesando estación:', name)
    try:
        path = app.save_station_by_name(df, name, from_date, to_date, output_dir=OUT)
        if path:
            summary.append({'station': name, 'file': str(path)})
            print('  Guardado:', path)
        else:
            summary.append({'station': name, 'file': None})
            print('  No se encontraron datos o se creó marcador')
    except Exception as e:
        summary.append({'station': name, 'error': str(e)})
        print('  Error:', e)

# save summary
outf = OUT / f'named_stations_remote_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
with open(outf, 'w', encoding='utf-8') as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
print('\nResumen guardado en', outf)
print('Resumen:')
for s in summary:
    print(s)
