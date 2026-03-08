import traceback
import sys
from pathlib import Path
sys.path.insert(0, r'g:\1_PROYECTOS\WEBSCRAPING\Web_Scraping_SENAMHI_')
import app
import pandas as pd

try:
    df = app.get_stations(use_local=True)
    print('Stations loaded:', len(df))
    mask = df['estacion'].astype(str).str.contains('AYABACA', case=False, na=False)
    print('Matches AYABACA:', int(mask.sum()))
    if mask.any():
        idx = df[mask].index[0]
        print('Row:', df.loc[idx].to_dict())

    out = app.save_station_by_name(df, 'AYABACA', '2026-01-01', '2026-02-28')
    print('save_station_by_name returned:', out)
    if out:
        p = Path(out)
        print('Saved file exists:', p.exists(), 'size=', p.stat().st_size)
        try:
            d = pd.read_excel(out)
            print('Preview rows:', len(d))
            print(d.head(10))
        except Exception as e:
            print('Error reading saved file:', e)
except Exception:
    traceback.print_exc()
