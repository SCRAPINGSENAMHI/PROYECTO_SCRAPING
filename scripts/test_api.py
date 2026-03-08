import requests, json

def main():
    url = 'http://127.0.0.1:5000/api/stations'
    try:
        r = requests.get(url, timeout=10)
        print('STATUS', r.status_code)
        data = r.json()
        print('TOTAL_STATIONS:', len(data))
        for i, s in enumerate(data[:10]):
            print(i, s.get('name'), 'lat=', s.get('lat'), 'lon=', s.get('lon'))
    except Exception as e:
        print('ERROR', e)

if __name__ == '__main__':
    main()
