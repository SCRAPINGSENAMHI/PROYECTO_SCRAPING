from pathlib import Path
import sys
import requests
import pandas as pd
from datetime import datetime

base = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(base))

try:
    import app as scraper
except Exception as e:
    print('Error importing app:', e)
    raise

OUT = base / 'DATA' / 'outputs'
OUT.mkdir(parents=True, exist_ok=True)

stations = sys.argv[1:] if len(sys.argv) > 1 else ['LA OROYA','RICRAN','AUTISHA','VON HUMBOLDT','CAMPO DE MARTE','ÑAÑA']
months_to_test = ['201501','202401']  # test an early and a recent month

session = requests.Session()

for name in stations:
    print('\n=== Debug station:', name)
    df = scraper.get_stations(use_local=True)
    # find candidate rows by substring or normalized
    cand = df[df['estacion'].astype(str).str.contains(name, case=False, na=False)]
    if cand.empty:
        # try normalize
        import unicodedata
        def normalize(s):
            s2 = str(s or '').lower()
            s2 = ''.join(c for c in unicodedata.normalize('NFD', s2) if unicodedata.category(c) != 'Mn')
            return s2
        tgt = normalize(name)
        cand = df[[tgt in normalize(x) for x in df['estacion'].astype(str)]]
    if cand.empty:
        print('  No match in master for', name)
        continue
    print('  Candidates:', len(cand))
    for i, row in cand.iterrows():
        cod = row.get('cod') or ''
        cod_old = row.get('cod_old') or None
        ico = row.get('ico') or ''
        estado = row.get('estado') or ''
        print('  Trying row idx', i, 'cod=', cod, 'cod_old=', cod_old)
        for m in months_to_test:
            if cod_old:
                url = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={m}&t_e={ico}&estado={estado}&cod_old={cod_old}"
            else:
                url = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={m}&t_e={ico}&estado={estado}"
            try:
                r = session.get(url, timeout=30)
                fname = OUT / f'debug_{name.replace(" ","_")}_{i}_{m}.html'
                with open(fname, 'w', encoding='utf-8') as f:
                    f.write(r.text)
                print('    Saved', fname.name, 'status', r.status_code)
                # check for tables
                try:
                    tables = pd.read_html(r.text)
                    print('      tables found:', len(tables))
                except Exception as e:
                    print('      no tables:', e)
            except Exception as e:
                print('    request error:', e)

print('\nDebug complete. HTML files saved in', OUT)
