import pandas as pd
from pathlib import Path
p = Path(__file__).resolve().parents[1] / 'DATA' / 'Estaciones_Meteorológicas_Peru.xlsx'
print('FILE:', p)
if not p.exists():
    print('NOT FOUND')
    raise SystemExit(1)
try:
    df = pd.read_excel(p)
except Exception as e:
    print('READ ERROR:', e)
    raise
print('ROWS, COLS:', df.shape)
print('COLUMNS:')
for c in df.columns:
    print(' -', repr(c))
# check candidate coordinate columns
candidates = ['lat','latitud','LAT','LATITUD','Latitude','Y','lon','longitud','LON','LONGITUD','Longitude','X']
found = {}
for cand in candidates:
    for col in df.columns:
        if str(col).strip().lower() == str(cand).strip().lower():
            s = df[col]
            nonnull = s.dropna().shape[0]
            found[col] = nonnull
print('\nCOORDINATE CANDIDATES NON-NULL COUNTS:')
for k,v in found.items():
    print(k, v)
# also attempt fuzzy match for columns containing lat/lon
lat_cols = [c for c in df.columns if 'lat' in str(c).lower()]
lon_cols = [c for c in df.columns if 'lon' in str(c).lower() or 'long' in str(c).lower()]
print('\nLAT-LIKE COLUMNS:', lat_cols)
print('LON-LIKE COLUMNS:', lon_cols)
print('\nSAMPLE rows with any lat/lon non-null:')
if lat_cols and lon_cols:
    mask = df[lat_cols[0]].notna() & df[lon_cols[0]].notna()
    print('Rows with both non-null:', mask.sum())
    print(df.loc[mask, df.columns[:10]].head(10).to_dict(orient='records'))
else:
    print('No obvious lat/lon columns found')
