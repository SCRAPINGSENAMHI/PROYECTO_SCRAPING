from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

from app import get_stations, download_station_data, _normalize_downloaded_df


def try_download(row, from_date, to_date):
    candidates = []
    if pd.notna(row.get('cod')):
        cod = str(row.get('cod'))
        if pd.notna(row.get('ico')):
            candidates.append((cod, str(row.get('ico'))))
        if 'CONVENCIONAL' in row and pd.notna(row.get('CONVENCIONAL')):
            candidates.append((cod, str(row.get('CONVENCIONAL'))))
        if 'AUTOMATICA' in row and pd.notna(row.get('AUTOMATICA')):
            candidates.append((cod, str(row.get('AUTOMATICA'))))
        candidates.append((cod, None))

    if pd.notna(row.get('cod_old')):
        cod_old = str(row.get('cod_old'))
        candidates.append((cod_old, None))

    seen = set()
    for cod, ico in candidates:
        if (cod, ico) in seen:
            continue
        seen.add((cod, ico))
        print(f"Probando cod={cod} ico={ico}")
        df = download_station_data(cod, ico, row.get('estado'), row.get('cod_old'), from_date, to_date, verbose=True)
        if df is not None and len(df) > 0:
            return df, cod, ico
    return None, None, None


def main():
    station_name = 'HUANTA'
    from_date = '2015-01-01'
    to_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    print('Cargando lista remota de estaciones (use_local=False)...')
    df_stations = get_stations(use_local=False)

    found = df_stations[df_stations['estacion'].str.contains(station_name, case=False, na=False)]
    if found.empty:
        print('No se encontró HUANTA en la lista remota')
        return

    row = found.iloc[0]
    print('Estación encontrada (remota):')
    print(row[['estacion', 'cod', 'cod_old', 'ico', 'estado']].to_string())

    df, used_cod, used_ico = try_download(row, from_date, to_date)

    out_dir = Path(__file__).resolve().parents[0] / 'DATA' / 'outputs'
    out_dir.mkdir(parents=True, exist_ok=True)

    if df is None:
        print('✗ No se encontraron tablas para HUANTA en el periodo solicitado.')
        marker = pd.DataFrame([{
            'estacion': station_name,
            'from_date': from_date,
            'to_date': to_date,
            'status': 'no_data_remote_attempts'
        }])
        outpath = out_dir / f"HUANTA_no_data_remote_{to_date}.xlsx"
        marker.to_excel(outpath, index=False)
        print('Marcador guardado en:', outpath)
        return

    outpath = out_dir / f"HUANTA_{from_date}_to_{to_date}_cod{used_cod}_ico{used_ico}.xlsx"
    try:
        df_save = _normalize_downloaded_df(df)
    except Exception:
        df_save = df
    df_save.to_excel(outpath, index=False)
    print('✓ Guardado:', outpath)
    print('Registros:', len(df))


if __name__ == '__main__':
    main()
