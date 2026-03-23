from pathlib import Path
import pandas as pd
import importlib
import sys
sys.path.append(str(Path(__file__).resolve().parents[1]))

mod = importlib.import_module('app.app')

# obtener df de la API (local loader)
df_api = mod.get_stations(use_local=True)

# leer maestra original
mae_path = Path(__file__).resolve().parents[1] / 'DATA' / 'Maestra_de_estaciones_Senamhi.xlsx'
df_mae = pd.read_excel(mae_path)

# normalizar columnas para codigo
cols_mae = {c.lower(): c for c in df_mae.columns}
code_col_mae = None
for k in ['cod', 'codigo', 'codigo_estacion']:
    if k in cols_mae:
        code_col_mae = cols_mae[k]
        break
if code_col_mae is None:
    # fallback: buscar columna con 'cod' en el nombre
    for k,v in cols_mae.items():
        if 'cod' in k:
            code_col_mae = v
            break

cols_api = {c.lower(): c for c in df_api.columns}
code_col_api = None
for k in ['cod', 'codigo']:
    if k in cols_api:
        code_col_api = cols_api[k]
        break
if code_col_api is None:
    for k,v in cols_api.items():
        if 'cod' in k:
            code_col_api = v
            break

print('Maestra shape:', df_mae.shape)
if 'LATITUD' in df_mae.columns and 'LONGITUD' in df_mae.columns:
    print('Maestra lat/lon non-null:', df_mae.dropna(subset=['LATITUD','LONGITUD']).shape[0])
else:
    print('Maestra lat/lon non-null: N/A (columns not present)')
print('API df shape:', getattr(df_api, 'shape', 'no-shape'))

# Obtener sets de codigos

def extract_codes(df, code_col):
    codes = set()
    if code_col and code_col in df.columns:
        for v in df[code_col].tolist():
            try:
                s = str(v).strip().upper()
                if s not in ('', 'NAN', 'NONE'):
                    codes.add(s)
            except Exception:
                continue
    return codes

codes_mae = extract_codes(df_mae, code_col_mae)
codes_api = extract_codes(df_api, code_col_api)

print('Maestra codes count:', len(codes_mae))
print('API codes count:', len(codes_api))

missing_in_api = sorted(list(codes_mae - codes_api))
missing_in_mae = sorted(list(codes_api - codes_mae))

print('Missing in API (from Maestra) count:', len(missing_in_api))
print('Missing in Maestra (in API) count:', len(missing_in_mae))

print('\nFirst 50 missing in API:')
for c in missing_in_api[:50]:
    print(c)

print('\nFirst 50 extra in API:')
for c in missing_in_mae[:50]:
    print(c)

# Mostrar algunas filas problemáticas de la maestra
if missing_in_api:
    sample = df_mae[df_mae[code_col_mae].astype(str).str.strip().str.upper().isin(missing_in_api[:10])]
    print('\nSample rows from Maestra for missing codes:')
    print(sample.head(10).to_string(index=False))

print('\nDone')
