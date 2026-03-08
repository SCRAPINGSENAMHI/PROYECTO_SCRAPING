import sys, json
from pathlib import Path
import pandas as pd

path_arg = sys.argv[1] if len(sys.argv) > 1 else r'G:\1_PROYECTOS\WEBSCRAPING\Web_Scraping_SENAMHI_\DATA\outputs\0024_CHUSIS_105105.xlsx'
P = Path(path_arg)
if not P.exists():
    print(json.dumps({'error': 'file not found', 'path': str(P)}))
    sys.exit(1)
try:
    df = pd.read_excel(P)
    rows = len(df)
    cols = [str(c) for c in df.columns]
    sample = df.head(10).replace({pd.NaT: None}).fillna('').to_dict(orient='records')
    out = {'path': str(P), 'rows': rows, 'columns': cols, 'sample': sample}
    print(json.dumps(out, ensure_ascii=False, indent=2))
except Exception as e:
    print(json.dumps({'error': str(e)}))
    sys.exit(1)
