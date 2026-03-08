import sys, traceback
from pathlib import Path
sys.path.insert(0, r'g:\1_PROYECTOS\WEBSCRAPING\Web_Scraping_SENAMHI_')
import app
import pandas as pd

station = sys.argv[1] if len(sys.argv) > 1 else 'CHUSIS'
from_date = sys.argv[2] if len(sys.argv) > 2 else '2026-01-01'
to_date = sys.argv[3] if len(sys.argv) > 3 else '2026-02-28'

print('Station:', station, 'from', from_date, 'to', to_date)
try:
    df = app.get_stations(use_local=True)
    print('Stations loaded:', len(df))
    mask = df['estacion'].astype(str).str.contains(station, case=False, na=False)
    print('Matches:', int(mask.sum()))
    if mask.any():
        idx = df[mask].index[0]
        print('Row:', df.loc[idx].to_dict())

    out = app.save_station_by_name(df, station, from_date, to_date)
    print('save_station_by_name returned:', out)
    if out:
        p = Path(out)
        print('Saved file exists:', p.exists(), 'size=', p.stat().st_size)
        try:
            d = pd.read_excel(out)
            print('Preview rows:', len(d))
            print(d.head(6))
        except Exception as e:
            print('Error reading saved file:', e)
except Exception:
    traceback.print_exc()
