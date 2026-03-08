import urllib.request, json, sys
from pathlib import Path

def fetch(layer):
    url = f'http://127.0.0.1:5000/api/geojson?layer={layer}'
    try:
        with urllib.request.urlopen(url, timeout=10) as r:
            return json.load(r)
    except Exception as e:
        print(f'ERROR fetching {layer}:', e)
        return None

def show_sample(geo):
    if not geo or 'features' not in geo:
        print(' no geo or no features')
        return
    feats = geo['features']
    print(' features_count=', len(feats))
    f = feats[0]
    geom = f.get('geometry')
    print(' geom_type=', geom.get('type'))
    coords = geom.get('coordinates')
    # try to print a small sample of numeric values
    def sample(obj, depth=0):
        if isinstance(obj, list) and depth<3:
            if not obj:
                return obj
            return [sample(obj[0], depth+1)]
        return obj
    print(' coords_sample=', sample(coords))

base = Path(__file__).resolve().parents[1] / 'DATA'

for layer in ('cuencas','sectores'):
    print('\n== LAYER', layer, '==')
    geo = fetch(layer)
    show_sample(geo)

# show prj files
prj_files = [
    base / 'CUENCAS' / 'UH.prj',
    base / 'SECTOR_CLIMATICO' / 'SECTORES.prj'
]
for p in prj_files:
    print('\nPRJ:', p)
    if p.exists():
        txt = p.read_text(encoding='utf-8', errors='ignore')
        print(txt.strip()[:1000])
    else:
        print('  (not found)')

print('\nDone')
