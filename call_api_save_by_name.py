import json
import sys
from urllib import request as urlreq
url = 'http://127.0.0.1:5000/api/save_by_name'
payload = {"station_name":"CHUSIS","from_date":"2026-01-01","to_date":"2026-02-28","use_local":True}
data = json.dumps(payload).encode('utf-8')
req = urlreq.Request(url, data=data, headers={'Content-Type':'application/json'})
try:
    with urlreq.urlopen(req, timeout=120) as resp:
        b = resp.read()
        print(resp.status)
        print(b.decode('utf-8', errors='replace'))
except Exception as e:
    print('ERROR', e)
    sys.exit(1)
