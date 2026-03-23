from pathlib import Path
import pandas as pd
import importlib
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
try:
    mod = importlib.import_module('app.app')
except Exception as e:
    print('import error', e); raise
# call get_stations
try:
    df = mod.get_stations(use_local=True)
    # if it's a Flask response, try to convert
    if hasattr(df, 'to_dict'):
        print('DataFrame shape:', getattr(df, 'shape', 'no-shape'))
        print('rows with lat/lon non-null:', df.dropna(subset=['lat','lon']).shape[0])
    else:
        # maybe it returned a JSON response
        try:
            print('type:', type(df))
            print('len:', len(df))
        except Exception as e:
            print('could not determine df shape', e)
except Exception as e:
    print('call error', e)
    raise
