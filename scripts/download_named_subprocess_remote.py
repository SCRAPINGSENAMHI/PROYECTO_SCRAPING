from pathlib import Path
from datetime import datetime
import json
import sys
import subprocess
import os

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

helper = base / 'scripts' / 'download_one_remote.py'

for name in stations:
    print('\n==============================')
    print('Procesando estación (subprocess):', name)
    try:
        cmd = [sys.executable, str(helper), name, from_date, to_date, str(OUT), str(base)]
        # timeout por estación (segundos)
        timeout = 900
        # ensure child process outputs utf-8 to avoid encoding errors on Windows
        env = dict(**os.environ)
        env['PYTHONIOENCODING'] = 'utf-8'
        # run without capturing output; helper writes a result JSON file we will read
        subprocess.run(cmd, timeout=timeout, env=env)
        # read result file written by helper
        safe_name = ''.join([c if c.isalnum() or c in (' ', '-', '_') else '_' for c in name])
        rf = OUT / f"{safe_name}_result.json"
        if rf.exists():
            try:
                with open(rf, 'r', encoding='utf-8') as _f:
                    info = json.load(_f)
            except Exception as e:
                info = {'station': name, 'error': f'failed_read_result: {e}'}
        else:
            info = {'station': name, 'error': 'no_result_file'}
        print('  result:', info)
        summary.append(info)
    except subprocess.TimeoutExpired:
        print('  Timeout descargando', name)
        summary.append({'station': name, 'error': 'timeout'})
    except Exception as e:
        print('  Error ejecutando helper:', e)
        summary.append({'station': name, 'error': str(e)})

# save summary
outf = OUT / f'named_stations_subproc_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json'
with open(outf, 'w', encoding='utf-8') as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
print('\nResumen guardado en', outf)
print('Resumen:')
for s in summary:
    print(s)
