import sys
try:
    import requests
except Exception:
    requests = None

url = 'http://127.0.0.1:5000/api/maestra_list'
if requests:
    try:
        r = requests.get(url, timeout=10)
        print('STATUS', r.status_code)
        print(r.text[:5000])
    except Exception as e:
        print('REQ_ERR', e)
        sys.exit(1)
else:
    import urllib.request, urllib.error
    try:
        r = urllib.request.urlopen(url, timeout=10)
        data = r.read().decode('utf-8')
        print(data[:5000])
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='ignore')
        print('HTTP', e.code)
        print(body[:5000])
    except Exception as e:
        print('ERR', e)
        sys.exit(1)
