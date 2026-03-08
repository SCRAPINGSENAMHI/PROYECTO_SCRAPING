from datetime import datetime
from pathlib import Path
import pandas as pd

from app import get_stations, get_station_data, _normalize_downloaded_df


def main():
    station_name = 'ORCOPAMPA'
    from_date = '2015-01-01'
    to_date = datetime.now().strftime('%Y-%m-%d')

    print(f"Cargando lista de estaciones (local o remota)...")
    df_stations = get_stations(use_local=True)

    # Si la columna 'ico' está vacía para la estación, intentar deducirla
    found = df_stations[df_stations['estacion'].str.contains(station_name, case=False, na=False)]
    if not found.empty:
        idx = found.index[0]
        if not df_stations.at[idx, 'ico']:
            # Preferir columna 'CONVENCIONAL' o 'AUTOMATICA' si existen
            if 'CONVENCIONAL' in df_stations.columns and pd.notna(df_stations.at[idx, 'CONVENCIONAL']):
                df_stations.at[idx, 'ico'] = str(df_stations.at[idx, 'CONVENCIONAL'])
            elif 'AUTOMATICA' in df_stations.columns and pd.notna(df_stations.at[idx, 'AUTOMATICA']):
                df_stations.at[idx, 'ico'] = str(df_stations.at[idx, 'AUTOMATICA'])

    print(f"Buscando y descargando datos para: {station_name} ({from_date} → {to_date})")
    df = get_station_data(station_name, from_date=from_date, to_date=to_date, df_stations=df_stations)

    out_dir = Path(__file__).resolve().parents[0] / 'DATA' / 'outputs'
    out_dir.mkdir(parents=True, exist_ok=True)

    if df is None or len(df) == 0:
        print('No se obtuvieron registros para la estación.')
        # escribir marcador
        meta = pd.DataFrame([{
            'estacion': station_name,
            'from_date': from_date,
            'to_date': to_date,
            'status': 'no_data'
        }])
        outpath = out_dir / f"ORCOPAMPA_no_data_{to_date}.xlsx"
        meta.to_excel(outpath, index=False)
        print('Se creó marcador en:', outpath)
        return

    # Guardar datos completos
    outpath = out_dir / f"ORCOPAMPA_{from_date}_to_{to_date}.xlsx"
    try:
        try:
            df_save = _normalize_downloaded_df(df)
        except Exception:
            df_save = df
        df_save.to_excel(outpath, index=False)
        print(f"Guardado: {outpath} ({len(df)} registros)")
    except Exception as e:
        print('Error guardando archivo:', e)

    # Mostrar resumen
    print('\nPrimeras filas:')
    try:
        print(df.head().to_string())
    except Exception:
        print(df.head())


if __name__ == '__main__':
    main()
