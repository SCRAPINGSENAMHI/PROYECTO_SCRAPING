from pathlib import Path
import importlib, sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
mod = importlib.import_module('app.app')
base = Path(__file__).resolve().parents[1] / 'DATA'
file1 = base / 'Estaciones_Meteorológicas_Peru.xlsx'
file2 = base / 'Maestra_de_estaciones_Senamhi.xlsx'

print('file1 exists', file1.exists())
print('file2 exists', file2.exists())

try:
    df1 = mod.load_local_stations(file1)
    print('file1 shape ->', getattr(df1,'shape', 'no-shape'))
except Exception as e:
    print('file1 error', e)

try:
    df2 = mod.load_local_stations(file2)
    print('file2 shape ->', getattr(df2,'shape', 'no-shape'))
except Exception as e:
    print('file2 error', e)

print('done')
