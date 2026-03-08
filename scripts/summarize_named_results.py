from pathlib import Path
import pandas as pd
import json

base = Path(__file__).resolve().parents[1]
OUT = base / 'DATA' / 'outputs'
stations = ['LA OROYA','RICRAN','AUTISHA','VON HUMBOLDT','CAMPO DE MARTE','ÑAÑA']
results = []
for s in stations:
    found = None
    for p in OUT.glob('*.xlsx'):
        if s.replace('Ñ','N').lower() in p.stem.replace('ñ','n').lower() or s.split()[0].lower() in p.stem.lower():
            found = p
            break
    if not found:
        results.append({'station': s, 'found': False})
        continue
    try:
        df = pd.read_excel(found)
        date_col = None
        for c in df.columns:
            lc = str(c).lower()
            if 'fecha' in lc or 'date' in lc or 'time' in lc:
                date_col = c; break
        if date_col is None:
            for c in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[c]):
                    date_col = c; break
        if date_col:
            sdates = pd.to_datetime(df[date_col], errors='coerce')
            if sdates.notna().any():
                dmin = str(sdates.min().date())
                dmax = str(sdates.max().date())
            else:
                dmin = dmax = None
        else:
            dmin = dmax = None
        results.append({'station': s, 'found': True, 'file': found.name, 'rows': len(df), 'date_min': dmin, 'date_max': dmax})
    except Exception as e:
        results.append({'station': s, 'found': True, 'file': found.name, 'error': str(e)})

print(json.dumps(results, ensure_ascii=False, indent=2))
