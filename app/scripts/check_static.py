import urllib.request
import sys
url = 'http://127.0.0.1:5000/static/dashboard.css'
try:
    req = urllib.request.Request(url, method='HEAD')
    with urllib.request.urlopen(req, timeout=5) as resp:
        code = resp.getcode()
        ctype = resp.info().get_content_type()
        print(code)
        print(ctype)
except Exception as e:
    print('ERROR', repr(e))
    sys.exit(2)
