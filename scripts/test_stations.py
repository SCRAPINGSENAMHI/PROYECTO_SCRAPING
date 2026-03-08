import urllib.request, json
url='http://127.0.0.1:5000/api/stations'
try:
    with urllib.request.urlopen(url, timeout=10) as r:
        j=json.loads(r.read())
        print('count=', len(j))
        if len(j):
            import pprint
            pprint.pprint(j[0])
except Exception as e:
    print('ERROR', e)
