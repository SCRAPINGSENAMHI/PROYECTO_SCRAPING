from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

p = ROOT / 'DATA' / 'outputs' / '0910_ACOBAMBA_112067.xlsx'
print('File:', p)
print('Exists:', p.exists())
if not p.exists():
    sys.exit(2)

try:
    df = pd.read_excel(p)
except Exception as e:
    print('Error reading excel:', e)
    sys.exit(3)

print('Shape:', df.shape)
print('Columns:', list(df.columns))
print('\nDtypes:')
print(df.dtypes)

print('\nFirst 10 rows:')
print(df.head(10).to_string(index=False))

# detect date-like columns and non-empty numeric rows
for c in df.columns:
    try:
        s = pd.to_datetime(df[c], errors='coerce')
        if s.notna().any():
            print(f"Detected datetime column: {c} -> min {s.min()} max {s.max()}")
    except Exception:
        pass

# Check for metadata marker
if 'status' in [str(c).lower() for c in df.columns]:
    print('\nFound status column; likely a metadata marker indicating no data')

print('\nDone')
