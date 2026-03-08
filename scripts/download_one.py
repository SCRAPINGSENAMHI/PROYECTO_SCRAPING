import sys
from pathlib import Path
import json

if len(sys.argv) < 6:
    print(json.dumps({'error': 'usage: download_one.py <station_name> <from_date> <to_date> <out_dir> <project_base>'}))
    sys.exit(2)

station_name = sys.argv[1]
from_date = sys.argv[2]
to_date = sys.argv[3]
out_dir = Path(sys.argv[4])
project_base = Path(sys.argv[5])

# ensure project base on path
import sys as _sys
_sys.path.insert(0, str(project_base))

try:
    import app as scraper
    import pandas as pd
except Exception as e:
    print(json.dumps({'error': f'import_error: {str(e)}'}))
    sys.exit(3)

try:
    df = scraper.get_stations(use_local=True)
    df = df.reset_index(drop=True)
    path = scraper.save_station_by_name(df, station_name, from_date, to_date, output_dir=out_dir)
    if path:
        # try to get basic info
        try:
            d = pd.read_excel(path)
            date_col = None
            for c in d.columns:
                lc = str(c).lower()
                if 'fecha' in lc or 'date' in lc or 'time' in lc:
                    date_col = c; break
            if date_col is None:
                for c in d.columns:
                    if pd.api.types.is_datetime64_any_dtype(d[c]):
                        date_col = c; break
            if date_col:
                s = pd.to_datetime(d[date_col], errors='coerce')
                dmin = str(s.min().date()) if s.notna().any() else None
                dmax = str(s.max().date()) if s.notna().any() else None
            else:
                dmin = dmax = None
            rows = len(d)
        except Exception:
            dmin = dmax = None; rows = None
        print(json.dumps({'station': station_name, 'file': str(path), 'rows': rows, 'date_min': dmin, 'date_max': dmax}))
        sys.exit(0)
    else:
        print(json.dumps({'station': station_name, 'file': None, 'rows': 0}))
        sys.exit(0)
except Exception as e:
    print(json.dumps({'station': station_name, 'error': str(e)}))
    sys.exit(4)
