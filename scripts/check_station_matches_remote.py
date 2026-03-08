from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import app

names = ['LA OROYA','RICRAN','AUTISHA','VON HUMBOLDT','CAMPO DE MARTE','ÑAÑA']

df = app.get_stations(use_local=False)

for n in names:
    matches = df[df['estacion'].str.contains(n, case=False, na=False)]
    print('\n---', n, 'matches:', len(matches))
    if len(matches) > 0:
        print(matches[['estacion','cod','ico','cod_old','lat','lon']].to_string(index=False))
    else:
        print('No matches')
