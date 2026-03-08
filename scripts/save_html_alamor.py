import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import get_stations
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

OUTDIR = Path(__file__).resolve().parents[0].parent / 'DATA' / 'outputs'
OUTDIR.mkdir(parents=True, exist_ok=True)

def main():
    df = get_stations(use_local=True)
    mask = df['estacion'].str.contains('ALAMOR', case=False, na=False)
    found = df[mask]
    if found.empty:
        print('No se encontró ALAMOR')
        return

    row = found.iloc[0]
    cod = row.get('cod')
    ico = row.get('ico')
    estado = row.get('estado')
    cod_old = row.get('cod_old')

    tsw = '202401'
    if pd.isna(cod_old):
        link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={tsw}&t_e={ico}&estado={estado}"
    else:
        link = f"https://www.senamhi.gob.pe//mapas/mapa-estaciones-2/export.php?estaciones={cod}&CBOFiltro={tsw}&t_e={ico}&estado={estado}&cod_old={cod_old}"

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    print('Solicitando:', link)
    resp = session.get(link, timeout=30)
    print('HTTP', resp.status_code)

    outpath = OUTDIR / f"ALAMOR_202401_response.html"
    with open(outpath, 'w', encoding='utf-8') as f:
        f.write(resp.text)

    print('Guardado en:', outpath)

if __name__ == '__main__':
    import pandas as pd
    main()    
