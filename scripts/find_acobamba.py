import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
import app

name = 'ACOBAMBA'
print('Buscando en maestra local (use_local=True)')
df_local = app.get_stations(use_local=True)
matches_local = df_local[df_local['estacion'].astype(str).str.contains(name, case=False, na=False)]
print('Matches local:', len(matches_local))
if len(matches_local):
    print(matches_local[['estacion','cod','cod_old','ico','categoria','estado']].to_string(index=False))

print('\nBuscando en lista remota (use_local=False)')
df_remote = app.get_stations(use_local=False)
matches_remote = df_remote[df_remote['estacion'].astype(str).str.contains(name, case=False, na=False)]
print('Matches remote:', len(matches_remote))
if len(matches_remote):
    print(matches_remote[['estacion','cod','cod_old','ico','categoria','estado']].to_string(index=False))

print('\nDone')
