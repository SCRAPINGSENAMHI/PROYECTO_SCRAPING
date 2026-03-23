import urllib.request, urllib.error
layers = ['cuencas', 'sectores', 'departamentos']
for layer in layers:
    url = f'http://127.0.0.1:5000/api/geojson?layer={layer}'
    try:
        r = urllib.request.urlopen(url, timeout=30)
        data = r.read().decode()
        print('---', layer)
        print(data[:1000])
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors='ignore')
        print('---', layer, 'HTTP', e.code)
        print(body[:1000])
    except Exception as e:
        print('---', layer, 'ERR', e)
