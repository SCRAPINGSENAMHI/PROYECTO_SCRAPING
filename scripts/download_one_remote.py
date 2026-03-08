import sys
from pathlib import Path
import json

if len(sys.argv) < 6:
    print(json.dumps({'error': 'usage: download_one_remote.py <station_name> <from_date> <to_date> <out_dir> <project_base>'}))
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
    df = scraper.get_stations(use_local=False)
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
        result = {'station': station_name, 'file': str(path), 'rows': rows, 'date_min': dmin, 'date_max': dmax}
        # save result file next to outputs
        safe_name = ''.join([c if c.isalnum() or c in (' ', '-', '_') else '_' for c in station_name])
        rf = out_dir / f"{safe_name}_result.json"
        with open(rf, 'w', encoding='utf-8') as _f:
            json.dump(result, _f, ensure_ascii=False)
        print('RESULT_FILE:' + str(rf))
        sys.exit(0)
    else:
        result = {'station': station_name, 'file': None, 'rows': 0}
        safe_name = ''.join([c if c.isalnum() or c in (' ', '-', '_') else '_' for c in station_name])
        rf = out_dir / f"{safe_name}_result.json"
        with open(rf, 'w', encoding='utf-8') as _f:
            json.dump(result, _f, ensure_ascii=False)
        print('RESULT_FILE:' + str(rf))
        sys.exit(0)
except Exception as e:
    result = {'station': station_name, 'error': str(e)}
    safe_name = ''.join([c if c.isalnum() or c in (' ', '-', '_') else '_' for c in station_name])
    rf = out_dir / f"{safe_name}_result.json"
    try:
        with open(rf, 'w', encoding='utf-8') as _f:
            json.dump(result, _f, ensure_ascii=False)
    except Exception:
        pass
    print('RESULT_FILE:' + str(rf))
    sys.exit(4)
