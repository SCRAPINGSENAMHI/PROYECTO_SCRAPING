import pandas as pd
from pathlib import Path
p = Path(__file__).resolve().parents[1] / 'DATA' / 'Estaciones_Meteorológicas_Peru.xlsx'
print('FILE:', p)
if not p.exists():
    print('NOT FOUND')
    raise SystemExit(1)
try:
    df0 = pd.read_excel(p, header=None)
except Exception as e:
    print('READ ERROR:', e)
    raise
print('shape:', df0.shape)
with pd.option_context('display.max_rows',20, 'display.max_columns',20):
    print(df0.head(20).to_string(index=False))
# show rows that contain 'LAT' or 'LON' strings anywhere
mask = df0.apply(lambda row: row.astype(str).str.contains('LAT|LON|Longitud|Latitud|LONG', case=False, na=False).any(), axis=1)
print('\nRows containing LAT/LON keywords (first 10):')
print(df0[mask].head(10).to_string(index=False))
