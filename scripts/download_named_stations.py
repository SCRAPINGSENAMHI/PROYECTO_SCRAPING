from pathlib import Path
from datetime import datetime
import sys
import json
import subprocess

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

helper = base / 'scripts' / 'download_one.py'

for name in stations:
    print('\n==============================')
    print('Procesando estación (subprocess):', name)
    try:
        cmd = [sys.executable, str(helper), name, from_date, to_date, str(OUT), str(base)]
        # timeout por estación (segundos)
        timeout = 1200
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        out = proc.stdout.strip() or proc.stderr.strip()
        try:
            info = json.loads(out)
        except Exception:
            info = {'station': name, 'raw': out}
        print('  result:', info)
        summary.append(info)
    except subprocess.TimeoutExpired:
        print('  Timeout descargando', name)
        summary.append({'station': name, 'error': 'timeout'})
    except Exception as e:
        print('  Error ejecutando helper:', e)
        summary.append({'station': name, 'error': str(e)})

# save summary
outf = OUT / f'named_stations_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
with open(outf, 'w', encoding='utf-8') as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
print('\nResumen guardado en', outf)
print('Resumen:')
for s in summary:
    print(s)
