import pandas as pd
from pathlib import Path
base = Path(__file__).resolve().parents[1] / 'DATA'
files = ['Estaciones_Meteorológicas_Peru.xlsx','Maestra_de_estaciones_Senamhi.xlsx']
for f in files:
    p = base / f
    if not p.exists():
        print(f, 'NOT FOUND')
        continue
    try:
        df = pd.read_excel(p)
    except Exception as e:
        print(f, 'READ ERROR', e)
        continue
    print('\nFILE:', f)
    print('shape:', df.shape)
    # try detect lat/lon columns
    lat_cols = [c for c in df.columns if 'lat' in str(c).lower()]
    lon_cols = [c for c in df.columns if 'lon' in str(c).lower() or 'long' in str(c).lower()]
    print('lat-like cols', lat_cols)
    print('lon-like cols', lon_cols)
    if lat_cols and lon_cols:
        latc = lat_cols[0]; lonc = lon_cols[0]
        ok = df[latc].notna() & df[lonc].notna()
        print('rows with lat/lon non-null:', ok.sum())
    else:
        # fallback: try use any numeric columns as coordinate heuristics
        print('No direct lat/lon columns found; showing head for inspection:')
        print(df.head(5).to_string())
