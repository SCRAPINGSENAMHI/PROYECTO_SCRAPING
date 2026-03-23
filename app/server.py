from flask import Flask, send_from_directory, jsonify, request
from pathlib import Path
import random
import shapefile
import json
import pandas as pd
import math
import time
import threading
import uuid
import requests

app = Flask(__name__, static_folder='static')

# --- Async download job queue ---
_download_jobs = {}   # job_id -> {status, result, error}
_jobs_lock = threading.Lock()


def _load_scraper_module():
    """Intento robusto de cargar el módulo scraper.

    Se prueba en este orden:
    1. import app (caso legacy cuando app.py estaba en root)
    2. import app.app (cuando el código está en el subpaquete app/)
    3. cargar desde el archivo app.py en la misma carpeta que server.py
    """
    import importlib
    import importlib.util
    from pathlib import Path as _P

    # 1) intentar importar 'app'
    try:
        mod = importlib.import_module('app')
        if hasattr(mod, 'get_stations'):
            return mod
    except Exception as e:
        print(f"_load_scraper_module: import 'app' failed: {e}")
        import traceback as _tb; _tb.print_exc()

    # 2) intentar importar 'app.app'
    try:
        mod = importlib.import_module('app.app')
        if hasattr(mod, 'get_stations'):
            return mod
    except Exception as e:
        print(f"_load_scraper_module: import 'app.app' failed: {e}")
        import traceback as _tb; _tb.print_exc()

    # 3) intentar cargar dinámicamente desde app.py en el mismo directorio
    try:
        p = _P(__file__).resolve().parents[0] / 'app.py'
        if p.exists():
            try:
                spec = importlib.util.spec_from_file_location('local_app_module', str(p))
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                if hasattr(mod, 'get_stations'):
                    return mod
            except Exception as e:
                print(f"_load_scraper_module: loading from file {p} failed: {e}")
                import traceback as _tb; _tb.print_exc()
    except Exception as e:
        print(f"_load_scraper_module: unexpected error: {e}")
        import traceback as _tb; _tb.print_exc()

    return None


scraper = _load_scraper_module()


_MAESTRA_FILES = ['Maestra_de_estaciones_Senamhi.xlsx', 'Estaciones_Meteorológicas_Peru.xlsx']

def _find_data_dir():
    """Buscar la carpeta `DATA` que contenga la Maestra de estaciones.
    Soporta Docker (/app/DATA) y local Windows (Web_Scraping_SENAMHI_/DATA).
    Prefiere directorios que tengan los archivos Excel reales sobre directorios vacios.
    """
    p = Path(__file__).resolve().parent
    candidates = []
    for i in range(4):
        cand = p
        for _ in range(i):
            cand = cand.parent
        candidates.append(cand / 'DATA')

    # Primero: buscar un directorio que tenga la Maestra
    for data_dir in candidates:
        if data_dir.exists() and any((data_dir / f).exists() for f in _MAESTRA_FILES):
            return data_dir
    # Segundo: cualquier directorio DATA que exista
    for data_dir in candidates:
        if data_dir.exists() and data_dir.is_dir():
            return data_dir
    # fallback
    return p / 'DATA'


@app.route('/')
def index():
    return send_from_directory('.', 'dashboard_hidrometeo.html')


@app.route('/historico')
def historico():
    return send_from_directory('.', 'dashboard_historico.html')


@app.route('/api/stations_historico')
def api_stations_historico():
    """
    Lista de estaciones del portal histórico de SENAMHI.

    Fuente primaria : map_hist_data.php (stationList embebido en JS, ~570 estaciones)
    Fuente fallback : Estaciones_Meteorológicas_Peru.xlsx (237 estaciones, DMS)

    Cada estación incluye:
      estacion, cod (portal), cod_qc (qcNNNNNNNN), cod_ho (hoNNNNNNNN),
      lat, lon, departamento/altitud, categoria
    """
    # ── Intentar fuente primaria: portal map_hist_data.php ──
    mod = scraper or _load_scraper_module()
    stations = []
    source = 'unknown'
    if mod and hasattr(mod, 'get_stations_hist_portal'):
        try:
            stations = mod.get_stations_hist_portal()
            if stations:
                source = 'portal'
        except Exception as e:
            print(f"api_stations_historico: portal fetch failed: {e}")
            stations = []

    # ── Fallback: Excel local con DMS ──
    if not stations:
        try:
            base = _find_data_dir()
            excel_path = base / 'Estaciones_Meteorológicas_Peru.xlsx'
            if excel_path.exists():
                df = pd.read_excel(excel_path, header=None)
                if df.shape[1] >= 9:
                    def dms_to_dd(deg, minu, sec):
                        try:
                            d = float(deg); m = float(minu); s = float(sec)
                            sign = -1.0 if d < 0 else 1.0
                            return round(d + sign * (abs(m) / 60.0 + abs(s) / 3600.0), 5)
                        except Exception:
                            return None

                    for _, row in df.iterrows():
                        cod  = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else None
                        name = str(row.iloc[8]).strip() if pd.notna(row.iloc[8]) else None
                        elev = row.iloc[7] if pd.notna(row.iloc[7]) else None
                        lat  = dms_to_dd(row.iloc[1], row.iloc[2], row.iloc[3])
                        lon  = dms_to_dd(row.iloc[4], row.iloc[5], row.iloc[6])
                        if not name or not lat or not lon:
                            continue
                        num = (cod or '').lstrip('hHoO').lstrip('qQcC').zfill(8)
                        stations.append({
                            'estacion': name,
                            'cod': cod or '',
                            'cod_qc': f'qc{num}',
                            'cod_ho': cod if (cod or '').lower().startswith('ho') else f'ho{num}',
                            'lat': lat, 'lon': lon,
                            'altitud': int(elev) if elev is not None else None,
                            'departamento': None,
                            'categoria': 'Convencional',
                        })
                    source = 'excel'
        except Exception as e:
            import traceback
            return jsonify({'ok': False, 'error': str(e), 'tb': traceback.format_exc()}), 500

    return jsonify({'ok': True, 'total': len(stations), 'source': source, 'stations': stations})


@app.route('/api/debug_scraper')
def api_debug_scraper():
    """Diagnóstico: simula exactamente lo que api_save_by_name hace para encontrar una estación."""
    global scraper
    if scraper is None:
        scraper = _load_scraper_module()
    if scraper is None:
        return jsonify({'error': 'scraper not loaded'})
    try:
        station_name = request.args.get('name', 'ACOMAYO')
        station_code = request.args.get('code', '113038')

        df = scraper.get_stations(use_local=True)
        df_work = df.copy().reset_index(drop=True)

        # Exact same logic as _find_row_index in api_save_by_name
        iloc_idx = None
        step = 'start'
        by_code = station_code
        by_name = station_name
        if by_code:
            step = 'code_exact'
            mask = df_work['cod'].astype(str).str.strip().str.upper() == str(by_code).strip().upper()
            if mask.any():
                iloc_idx = df_work.index.get_loc(df_work[mask].index[0])
                step = 'code_exact_found'
            else:
                step = 'code_partial'
                mask2 = df_work['cod'].astype(str).fillna('').str.contains(str(by_code), case=False, na=False)
                if mask2.any():
                    iloc_idx = df_work.index.get_loc(df_work[mask2].index[0])
                    step = 'code_partial_found'
        if iloc_idx is None and by_name:
            step = 'name_search'
            mask = df_work['estacion'].astype(str).str.contains(str(by_name), case=False, na=False)
            if mask.any():
                iloc_idx = df_work.index.get_loc(df_work[mask].index[0])
                step = 'name_found'

        result_row = None
        if iloc_idx is not None:
            row = df_work.iloc[iloc_idx]
            result_row = {'estacion': str(row.get('estacion')), 'cod': str(row.get('cod')), 'ico': str(row.get('ico'))}

        return jsonify({
            'shape': list(df.shape),
            'iloc_idx': iloc_idx,
            'step': step,
            'found': iloc_idx is not None,
            'row': result_row,
            'index_type': type(df_work.index).__name__,
        })
    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'tb': traceback.format_exc()})

@app.route('/api/stations')
def api_stations():
    use_local = request.args.get('use_local', 'true').lower() == 'true'
    # Prefer reading local maestra directly (ensures coordinates present).
    base = _find_data_dir()
    # Support two possible local master files; prefer the new Estaciones_Meteorológicas_Peru.xlsx if present
    maestra_candidates = [
        base / 'Estaciones_Meteorológicas_Peru.xlsx',
        base / 'Maestra_de_estaciones_Senamhi.xlsx'
    ]
    df = None
    if use_local:
        for cand in maestra_candidates:
            if cand.exists():
                try:
                    df = pd.read_excel(cand)
                    print(f"api_stations: cargada maestra local: {cand}")
                    break
                except Exception as e:
                    print(f"api_stations: fallo leyendo {cand}: {e}")
                    df = None
                    continue

        # Prefer using the scraper's loader to ensure normalization (handles DMS, headers, etc.)
        if use_local:
            try:
                mod = scraper or _load_scraper_module()
                if mod and hasattr(mod, 'get_stations'):
                    df = mod.get_stations(use_local=True)
            except Exception:
                df = None
            # fallback: try direct read of known files (robust to headerless files)
            if df is None:
                maestra_candidates = [
                    base / 'Estaciones_Meteorológicas_Peru.xlsx',
                    base / 'Maestra_de_estaciones_Senamhi.xlsx'
                ]
                for cand in maestra_candidates:
                    if cand.exists():
                        try:
                            # read with header=None to avoid accidental header as data
                            df = pd.read_excel(cand, header=0)
                            break
                        except Exception:
                            try:
                                df = pd.read_excel(cand, header=None)
                                break
                            except Exception:
                                df = None
        else:
            # when not using local, call scraper remote list
            if scraper is not None:
                df = scraper.get_stations(use_local=False)

    if df is None:
        return jsonify([])
    stations = []
    # Priorizar columnas exactas de la maestra (mayúsculas/español)
    # Asegurar que los nombres de columna se traten como strings
    cols_upper = {str(c).upper(): c for c in df.columns}

    def colname(*keys_upper):
        for k in keys_upper:
            if k and k.upper() in cols_upper:
                return cols_upper[k.upper()]
        return None

    name_col = colname('NOMBRE_ESTACION', 'NOMBRE', 'ESTACION')
    cod_col = colname('CODIGO', 'COD')
    lat_col = colname('LATITUD', 'LAT', 'Y')
    lon_col = colname('LONGITUD', 'LON', 'LONG')
    alt_col = colname('ALTITUD')
    dept_col = colname('DEPARTAMENTO', 'DEPT')
    prov_col = colname('PROVINCIA')
    dist_col = colname('DISTRITO')
    hid_reg_col = colname('REGION_HIDROGRAFICA')
    hid_uni_col = colname('UNIDAD_HIDROGRAFICA')
    conv_col = colname('CONVENCIONAL')
    auto_col = colname('AUTOMATICA')
    estado_col = colname('ESTADO')
    # fallback picks
    ico_col = colname('ICO', 'TIPO', 'CATEGORIA')
    cuenca_col = hid_uni_col or hid_reg_col or colname('CUENCA')

    for idx, row in df.iterrows():
        try:
            lat = None
            lon = None
            if lat_col:
                lat = row.get(lat_col)
            if lon_col:
                lon = row.get(lon_col)
            # ensure numeric
            try:
                lat = float(lat) if lat is not None and str(lat).strip() != '' else None
            except Exception:
                lat = None
            try:
                lon = float(lon) if lon is not None and str(lon).strip() != '' else None
            except Exception:
                lon = None

            # determinar tipo: preferir columna AUTOMATICA / CONVENCIONAL si existen
            _EMPTY = {'', '0', 'false', 'nan', 'none', 'nat', 'false'}
            def _has_val(col):
                if not col: return False
                v = row.get(col)
                if v is None: return False
                try:
                    import math
                    if isinstance(v, float) and math.isnan(v): return False
                except Exception: pass
                return str(v).strip().lower() not in _EMPTY

            tipo_val = None
            try:
                if _has_val(auto_col):
                    tipo_val = 'Automática'
                elif _has_val(conv_col):
                    tipo_val = 'Convencional'
                else:
                    # fallback a búsqueda en ico_col
                    tipo_val = 'Automática' if (str(row.get(ico_col) or '').lower().find('auto') != -1) else 'Convencional'
            except Exception:
                tipo_val = 'Convencional'

            # Estado operativo: OPERATIVA | CLAUSURADA | SUSPENDIDA | PARCIALMENTE
            raw_estado = str(row.get(estado_col) or '').strip().upper() if estado_col else ''
            # Normalizar a 3 categorias para el frontend
            if raw_estado in ('OPERATIVA',):
                estado_operativo = 'OPERATIVA'
            elif raw_estado in ('CLAUSURADA', 'SUSPENDIDA'):
                estado_operativo = 'INACTIVA'
            elif raw_estado in ('PARCIALMENTE',):
                estado_operativo = 'PARCIAL'
            else:
                estado_operativo = 'DESCONOCIDO'

            stations.append({
                'name': (row.get(name_col) if name_col else None) or f"STN_{idx}",
                'tipo': tipo_val,
                'estado_operativo': estado_operativo,
                'dept': row.get(dept_col) if dept_col else None,
                'cuenca': (row.get(cuenca_col) if cuenca_col else None) or None,
                'prec': round(random.uniform(0, 40), 1),
                'temp': round(random.uniform(5, 32), 1),
                'act': int(random.uniform(60, 100)),
                'lat': lat,
                'lon': lon,
                'cod': row.get(cod_col) if cod_col else None,
                'provincia': row.get(prov_col) if prov_col else None,
                'distrito': row.get(dist_col) if dist_col else None,
                'altitud': row.get(alt_col) if alt_col else None
            })
        except Exception:
            continue
    # Si no hay estaciones válidas con lat/lon, devolver un conjunto de muestra
    has_coords = any(s.get('lat') is not None and s.get('lon') is not None for s in stations)
    if not stations or not has_coords:
        stations = [
            {'name':'LIMA - A', 'tipo':'Automática', 'dept':'LIMA', 'cuenca':'Rímac', 'prec':2.4, 'temp':18.5, 'act':88, 'lat':-12.0464, 'lon':-77.0428},
            {'name':'CUSCO - A', 'tipo':'Convencional', 'dept':'CUSCO', 'cuenca':'Urubamba', 'prec':6.1, 'temp':12.3, 'act':91, 'lat':-13.5319, 'lon':-71.9675},
            {'name':'IQUITOS - A', 'tipo':'Convencional', 'dept':'LORETO', 'cuenca':'Amazonas', 'prec':28.3, 'temp':30.2, 'act':72, 'lat':-3.7492, 'lon':-73.2536}
        ]

    # Clean stations recursively to replace non-serializable values (NaN, bytes)
    try:
        clean = _clean_for_json(stations)
        return app.response_class(json.dumps(clean, ensure_ascii=False), mimetype='application/json')
    except Exception:
        return app.response_class(json.dumps(stations, default=str, ensure_ascii=False), mimetype='application/json')



def shapefile_to_geojson(shp_path, filter_name=None):
    # Try reading shapefile with several encodings because DBF charsets vary
    enc_attempts = [None, 'utf-8', 'cp1252', 'latin-1']
    sf = None
    last_exc = None
    for enc in enc_attempts:
        try:
            if enc is None:
                sf = shapefile.Reader(str(shp_path))
            else:
                # pyshp supports encoding and encodingErrors kwargs
                sf = shapefile.Reader(str(shp_path), encoding=enc)
            # try to access records to trigger any decode errors early
            _ = sf.records()
            last_exc = None
            break
        except Exception as e:
            last_exc = e
            sf = None
            continue
    if sf is None:
        # final attempt with replace errors
        try:
            sf = shapefile.Reader(str(shp_path), encoding='latin-1', encodingErrors='replace')
        except Exception as e:
            raise last_exc or e
    # ensure field names are str (decode bytes if necessary)
    fields = []
    for f in sf.fields[1:]:
        name = f[0]
        if isinstance(name, (bytes, bytearray)):
            try:
                name = name.decode('utf-8')
            except Exception:
                try:
                    name = name.decode('cp1252')
                except Exception:
                    name = name.decode('latin-1', errors='replace')
        fields.append(name)
    features = []
    for sr, sh in zip(sf.records(), sf.shapes()):
        # decode byte values from DBF if needed (some shapefiles use latin1/cp1252)
        props = {}
        for k, v in zip(fields, sr):
            if isinstance(v, (bytes, bytearray)):
                try:
                    s = v.decode('utf-8')
                except Exception:
                    try:
                        s = v.decode('cp1252')
                    except Exception:
                        s = v.decode('latin-1', errors='replace')
                props[k] = s
            else:
                props[k] = v

        # optional simple filter by name appearing in any property
        if filter_name:
            try:
                import unicodedata as _unic
                def _norm(s):
                    try:
                        s2 = str(s or '')
                        s2 = ''.join(c for c in _unic.normalize('NFD', s2) if _unic.category(c) != 'Mn')
                        return s2.lower()
                    except Exception:
                        return str(s or '').lower()
                nf = _norm(filter_name)
                found = False
                for v in props.values():
                    try:
                        if nf in _norm(v):
                            found = True
                            break
                    except Exception:
                        continue
                if not found:
                    continue
            except Exception:
                # fallback to simple lowercase substring match
                found = False
                for v in props.values():
                    try:
                        if filter_name.lower() in str(v).lower():
                            found = True
                            break
                    except Exception:
                        continue
                if not found:
                    continue

        geom = None
        if sh.shapeType in (shapefile.POLYGON, shapefile.POLYGONZ, shapefile.POLYGONM):
            parts = list(sh.parts) + [len(sh.points)]
            rings = []
            for i in range(len(parts)-1):
                start, end = parts[i], parts[i+1]
                ring = [[pt[0], pt[1]] for pt in sh.points[start:end]]
                rings.append(ring)
            geom = {"type": "Polygon", "coordinates": rings}
        elif sh.shapeType in (shapefile.POINT, shapefile.POINTZ, shapefile.POINTM):
            geom = {"type": "Point", "coordinates": [sh.points[0][0], sh.points[0][1]]}
        elif sh.shapeType in (shapefile.MULTIPATCH,):
            geom = {"type": "Polygon", "coordinates": [[[pt[0], pt[1]] for pt in sh.points]]}
        else:
            # fallback - try poly
            try:
                geom = {"type": "Polygon", "coordinates": [[[pt[0], pt[1]] for pt in sh.points]]}
            except Exception:
                geom = None

        if geom is None:
            continue

        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": props
        })

    return {"type": "FeatureCollection", "features": features}


def _clean_for_json(obj):
    # recursively convert bytes to str so json.dumps won't fail
    if isinstance(obj, dict):
        return {(_clean_for_json(k) if isinstance(k, (bytes, bytearray)) else k): _clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_clean_for_json(v) for v in obj]
    # convert non-finite floats (NaN, inf) to None so JSON.parse won't fail
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    # bytes -> str
    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode('utf-8')
        except Exception:
            try:
                return obj.decode('cp1252')
            except Exception:
                return obj.decode('latin-1', errors='replace')
    return obj


def _saved_file_has_data(path):
    """Comprueba si el archivo Excel guardado contiene datos reales.

    Retorna True si el fichero existe y contiene filas útiles, False si está
    vacío o si la columna `status` indica un marcador de no-datos en todas
    sus filas.
    """
    try:
        import pandas as _pd
        p = Path(path)
        if not p.exists():
            return False
        df = _pd.read_excel(p)
        if df is None or len(df) == 0:
            return False
        if 'status' in df.columns:
            try:
                statuses = df['status'].astype(str).str.lower()
                if statuses.str.contains('no_data', na=False).all():
                    return False
            except Exception:
                pass
        return True
    except Exception:
        # En caso de error leyendo el fichero, asumir que NO hay datos
        return False


@app.route('/api/geojson')
def api_geojson():
    layer = (request.args.get('layer') or '').lower()
    name = request.args.get('name')
    base = _find_data_dir()

    if layer in ('cuencas','cuenca'):
        shp_path = base / 'CUENCAS' / 'UH.shp'
    elif layer in ('sectores','zona'):
        shp_path = base / 'SECTOR_CLIMATICO' / 'SECTORES.shp'
    elif layer in ('departamentos','depto','dept'):
        # prefer the specific INEI departamental shapefile if present
        d = base / 'DEPARTAMENTOS'
        preferred = d / 'INEI_LIMITE_DEPARTAMENTAL_GEOGPSPERU_JUANSUYO_931381206.shp'
        if preferred.exists():
            shp_path = preferred
        else:
            # fallback to first found
            files = list(d.glob('*.shp'))
            if not files:
                return jsonify({'error': 'no shapefile found in DEPARTAMENTOS'}), 404
            shp_path = files[0]
    else:
        return jsonify({'error': 'unknown layer'}), 400

    if not shp_path.exists():
        return jsonify({'error': f'shapefile not found: {shp_path}'}), 404

    try:
        geo = shapefile_to_geojson(shp_path, filter_name=name)
        geo = _clean_for_json(geo)
        return app.response_class(json.dumps(geo, ensure_ascii=False), mimetype='application/json')
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/site_downloads')
def api_site_downloads():
    """Lista enlaces de la página 'Descarga de datos' de SENAMHI.
    Usa la función `get_site_downloads` del módulo scraper si está disponible.
    """
    downloads = []
    try:
        if scraper and hasattr(scraper, 'get_site_downloads'):
            downloads = scraper.get_site_downloads()
        else:
            try:
                mod = _load_scraper_module()
                if mod and hasattr(mod, 'get_site_downloads'):
                    downloads = mod.get_site_downloads()
            except Exception:
                downloads = []
    except Exception:
        downloads = []
    return jsonify(downloads)


@app.route('/api/maestra_list')
def api_maestra_list():
    """Devuelve la lista de estaciones cargadas desde el archivo Maestra local.

    Response JSON:
      { ok: true, file: 'Estaciones_Meteorológicas_Peru.xlsx', stations: [ ... ] }
    o
      { ok: false, error: 'mensaje' }
    """
    try:
        mod = scraper or _load_scraper_module()
        if not mod:
            return jsonify({'ok': False, 'error': 'scraper module not available'}), 500
        # llamar a get_stations con use_local=True para forzar la lectura local
        # Prefer calling a direct loader if available to avoid side-effects
        df = None
        try:
            if hasattr(mod, 'load_local_stations'):
                try:
                    base = _find_data_dir()
                    # intentar cargar la Maestra oficial primero
                    cand = base / 'Maestra_de_estaciones_Senamhi.xlsx'
                    if not cand.exists():
                        cand = base / 'Estaciones_Meteorológicas_Peru.xlsx'
                    if cand.exists():
                        df = mod.load_local_stations(cand)
                except Exception:
                    df = None
            if df is None:
                df = mod.get_stations(use_local=True)
        except Exception as e:
            # get_stations puede devolver un Response (si fue llamado desde server context); intentar cargar directamente
            try:
                # intentar cargar usando pandas sobre archivos conocidos
                base = _find_data_dir()
                candidates = [base / 'Estaciones_Meteorológicas_Peru.xlsx', base / 'Maestra_de_estaciones_Senamhi.xlsx']
                df = None
                used = None
                import pandas as _pd
                for cand in candidates:
                    if cand.exists():
                        try:
                            df = _pd.read_excel(cand)
                            used = str(cand.name)
                            break
                        except Exception:
                            df = None
                if df is None:
                    return jsonify({'ok': False, 'error': 'no local maestra file found or failed to read'}), 404
            except Exception as e2:
                return jsonify({'ok': False, 'error': str(e2)}), 500

        # Si df es un response o DataFrame, normalizar a lista de dicts
        if hasattr(df, 'to_dict'):
            stations = df.to_dict(orient='records')
        elif isinstance(df, (list, tuple)):
            stations = list(df)
        else:
            stations = []

        # Sanitizar valores no serializables (pandas NaT/Timestamp, numpy types, bytes)
        try:
            import pandas as _pd
            import numpy as _np
            def _sanitize_value(v):
                if v is None:
                    return None
                # pandas NA/NaT (cubre NaT y NA)
                try:
                    if _pd.isna(v):
                        return None
                    if isinstance(v, _pd.Timestamp):
                        return str(v)
                except Exception:
                    pass
                # numpy scalars -> native python
                try:
                    if isinstance(v, (_np.integer, _np.floating, _np.bool_)):
                        return v.item()
                except Exception:
                    pass
                # bytes
                if isinstance(v, (bytes, bytearray)):
                    try:
                        return v.decode('utf-8')
                    except Exception:
                        return v.decode('latin-1', errors='replace')
                return v

            stations = [{k: _sanitize_value(v) for k, v in (r.items() if isinstance(r, dict) else {})} if isinstance(r, dict) else r for r in stations]
        except Exception:
            pass

        return jsonify({'ok': True, 'file': used if 'used' in locals() else None, 'stations': stations})
    except Exception as e:
        import traceback as _tb
        tb = _tb.format_exc()
        return jsonify({'ok': False, 'error': str(e), 'traceback': tb}), 500


@app.route('/api/outputs')
def api_outputs():
    base = _find_data_dir() / 'outputs'
    if not base.exists() or not base.is_dir():
        return jsonify([])
    files = []
    for p in sorted(base.glob('*'), key=lambda x: x.stat().st_mtime, reverse=True):
        try:
            files.append({
                'name': p.name,
                'path': str(p),
                'mtime': int(p.stat().st_mtime)
            })
        except Exception:
            continue
    return jsonify(files)


@app.route('/api/output_preview')
def api_output_preview():
    # preview saved output for a station (search by station name or code)
    station = (request.args.get('station') or '').strip().lower()
    base = _find_data_dir() / 'outputs'
    if not station or not base.exists():
        return jsonify({'found': False, 'error': 'missing station or outputs folder'}), 400

    # find candidate file (prefer exact contains)
    candidates = [p for p in base.glob('*.xlsx')]
    match = None
    for p in candidates:
        if station in p.name.lower():
            match = p
            break
    if match is None and candidates:
        # try code numeric match inside filename
        for p in candidates:
            if any(ch.isdigit() for ch in station) and any(d in p.name for d in station):
                match = p
                break

    if match is None:
        return jsonify({'found': False}), 404

    try:
        df = pd.read_excel(match)
    except Exception as e:
        return jsonify({'found': False, 'error': f'error reading file: {str(e)}'}), 500

    # basic summary
    rows = len(df)
    cols = list(df.columns.astype(str))
    sample = df.head(10).replace({pd.NaT: None}).fillna('').to_dict(orient='records')

    # detect date column
    date_col = None
    for c in cols:
        lc = c.lower()
        if 'fecha' in lc or 'date' in lc or 'time' in lc:
            date_col = c; break
    if date_col is None:
        # fallback to datetime dtype
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                date_col = str(c); break

    date_min = None; date_max = None; monthly = None
    if date_col is not None:
        try:
            s = pd.to_datetime(df[date_col], errors='coerce')
            if s.notna().any():
                date_min = str(s.min().date())
                date_max = str(s.max().date())
                # compute monthly mean of first numeric column if any
                numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
                if numeric_cols:
                    num = numeric_cols[0]
                    tmp = df[[date_col, num]].copy()
                    tmp[date_col] = pd.to_datetime(tmp[date_col], errors='coerce')
                    tmp = tmp.dropna(subset=[date_col])
                    if not tmp.empty:
                        tmp['ym'] = tmp[date_col].dt.to_period('M')
                        grp = tmp.groupby('ym')[num].mean().sort_index()
                        monthly = [{'period': str(idx), 'value': float(v) if not pd.isna(v) else None} for idx, v in grp.items()]
        except Exception:
            pass

    resp = {'found': True, 'file': match.name, 'path': str(match), 'rows': rows, 'columns': cols, 'sample': sample, 'date_min': date_min, 'date_max': date_max, 'monthly': monthly}
    return jsonify(resp)



@app.route('/api/save_by_name', methods=['POST'])
def api_save_by_name():
    data = request.json or {}
    name = data.get('station_name')
    code = data.get('station_code')
    from_date = data.get('from_date', '2024-01-01')
    to_date = data.get('to_date', '2024-12-31')
    use_local = data.get('use_local', True)

    # Código alternativo ho<->qc: el Excel usa 'ho', el portal SENAMHI usa 'qc'.
    # Si el frontend envía station_code_ho o station_code_qc, los usamos como fallback.
    code_ho = data.get('station_code_ho') or data.get('station_code_alt')
    # Normalizar: si 'code' empieza con ho/qc, generar su par automáticamente
    def _swap_prefix(c):
        if not c:
            return None
        cl = str(c).lower()
        if cl.startswith('ho'):
            return 'qc' + str(c)[2:]
        if cl.startswith('qc'):
            return 'ho' + str(c)[2:]
        return None
    code_alt = code_ho or _swap_prefix(code)

    # Campos opcionales que el dashboard puede enviar para mejorar la resolucion
    # de parametros del portal (t_e e estado en export.php)
    hint_tipo = data.get('tipo')          # 'Convencional' o 'Automatica'
    hint_estado_op = data.get('estado_operativo')  # 'OPERATIVA', 'INACTIVA', etc.
    hint_ico = data.get('ico')            # 'M' o 'H' si el cliente lo conoce

    # Intentar recargar el módulo scraper en tiempo de petición si aún no está cargado
    global scraper
    if scraper is None:
        scraper = _load_scraper_module()
        if scraper is None:
            return jsonify({'saved': False, 'error': 'scraper not available'}), 500
    try:
        df = scraper.get_stations(use_local=use_local)
    except Exception as e:
        return jsonify({'saved': False, 'error': f'error getting stations: {str(e)}'}), 500

    def normalize(s):
        try:
            import unicodedata
            s2 = str(s or '').lower()
            s2 = ''.join(c for c in unicodedata.normalize('NFD', s2) if unicodedata.category(c) != 'Mn')
            s2 = ''.join(ch for ch in s2 if ch.isalnum())
            return s2
        except Exception:
            return str(s or '').lower()

    def _patch_row_with_hints(df_local, row_idx):
        """Si el dashboard envio hints de tipo/ico, inyectarlos en la fila del DataFrame
        para que _resolve_portal_params() tenga mas informacion y elija mejor candidato."""
        if hint_ico and str(hint_ico).strip().upper() in ('M', 'H'):
            df_local.at[row_idx, 'ico'] = str(hint_ico).strip().upper()
        elif hint_tipo:
            tipo_low = str(hint_tipo).lower()
            if 'auto' in tipo_low:
                # Marcar columna AUTOMATICA con un valor generico si esta vacia
                if 'AUTOMATICA' in df_local.columns:
                    val = df_local.at[row_idx, 'AUTOMATICA']
                    if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
                        df_local.at[row_idx, 'AUTOMATICA'] = 'EMA'
            elif 'conv' in tipo_low:
                if 'CONVENCIONAL' in df_local.columns:
                    val = df_local.at[row_idx, 'CONVENCIONAL']
                    if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
                        df_local.at[row_idx, 'CONVENCIONAL'] = 'CO'
        return df_local

    def _find_row_index(df_local, by_code=None, by_name=None):
        """Devuelve el iloc-index (entero) de la primera fila que coincide.

        Maneja el intercambio de prefijos ho<->qc: el Excel usa 'ho', el portal
        SENAMHI usa 'qc' para el mismo número de estación.
        """
        def _try_code(c):
            if not c:
                return None
            mask = df_local['cod'].astype(str).str.strip().str.upper() == str(c).strip().upper()
            if mask.any():
                return df_local.index.get_loc(df_local[mask].index[0])
            mask2 = df_local['cod'].astype(str).fillna('').str.contains(str(c), case=False, na=False)
            if mask2.any():
                return df_local.index.get_loc(df_local[mask2].index[0])
            return None

        # Por codigo exacto primero (y su variante ho<->qc)
        if by_code:
            idx = _try_code(by_code)
            if idx is not None:
                return idx
            # Intentar variante de prefijo
            idx = _try_code(code_alt)
            if idx is not None:
                return idx
        # Por nombre (substring)
        if by_name:
            mask = df_local['estacion'].astype(str).str.contains(str(by_name), case=False, na=False)
            if mask.any():
                return df_local.index.get_loc(df_local[mask].index[0])
            # Por nombre normalizado
            tgt = normalize(by_name)
            for ix, row_iter in df_local.iterrows():
                try:
                    if tgt and normalize(row_iter.get('estacion') or '').find(tgt) != -1:
                        return df_local.index.get_loc(ix)
                except Exception:
                    continue
        return None

    def _attempt_save(df_src, use_remote_label='', portal_params=None):
        """Intenta localizar y guardar. Devuelve (path, error_str).

        portal_params: dict con ico/estado del portal (obtenido previamente con
                       _get_remote_params_for_station). Si se provee, se inyectan
                       en la fila para que download_station_data_robust use el
                       camino rápido (sin bruteforce ni re-consulta remota).
        """
        df_work = df_src.copy().reset_index(drop=True)
        iloc_idx = _find_row_index(df_work, by_code=code, by_name=name)
        if iloc_idx is None:
            return None, 'station not found'
        df_work = _patch_row_with_hints(df_work, iloc_idx)
        # Inyectar params del portal si se proveen (evita el bruteforce costoso)
        if portal_params and not hint_ico:
            df_work.at[iloc_idx, 'ico'] = portal_params.get('ico', '')
            df_work.at[iloc_idx, 'estado'] = portal_params.get('estado', '')
        try:
            path = scraper.save_station_by_index(df_work, iloc_idx, from_date, to_date)
        except Exception as exc:
            return None, str(exc)
        if path:
            # Verificar que el archivo guardado tenga datos reales (no solo el marcador de error)
            try:
                import pandas as _pd
                df_check = _pd.read_excel(path)
                # Si el DataFrame está vacío, tratar como no-data
                if df_check is None or len(df_check) == 0:
                    return None, f'Sin datos disponibles en SENAMHI{use_remote_label}'
                # Si existe una columna 'status', comprobar si TODOS los valores
                # indican no_data (evita falsos positivos cuando la tabla real
                # contiene una columna llamada 'status').
                if 'status' in df_check.columns:
                    try:
                        statuses = df_check['status'].astype(str).str.lower()
                        if statuses.str.contains('no_data', na=False).all():
                            return None, f'Sin datos disponibles en SENAMHI{use_remote_label}'
                    except Exception:
                        # Si algo falla en la comprobación, no bloquear el guardado
                        pass
            except Exception:
                pass
            return path, None
        return None, f'Sin datos disponibles en SENAMHI{use_remote_label}'

    try:
        # --- Pre-check: buscar parámetros reales del portal SENAMHI (síncrono, ~6s) ---
        # Esto da feedback inmediato si la estación no existe en el portal.
        portal_params = None

        def _is_excel_code(c):
            """Códigos ho*/qc* son del Excel local — no existen en el portal SENAMHI vivo."""
            if not c:
                return False
            return str(c).lower().startswith(('ho', 'qc'))

        check_cod = ''
        # Si el código recibido es formato Excel (ho*/qc*), no usarlo para el portal;
        # buscar la estación por nombre en la Maestra local para obtener el código real.
        if code and not _is_excel_code(code):
            check_cod = code
        elif name:
            # Buscar por nombre en la Maestra local → código real del portal
            try:
                df_tmp = df.copy().reset_index(drop=True)
                tmp_idx = _find_row_index(df_tmp, by_code=None, by_name=name)
                if tmp_idx is not None:
                    found_cod = str(df_tmp.iloc[tmp_idx].get('cod') or '').strip()
                    if found_cod and not _is_excel_code(found_cod):
                        check_cod = found_cod
            except Exception:
                pass

        if check_cod and hasattr(scraper, '_get_remote_params_for_station'):
            try:
                portal_params = scraper._get_remote_params_for_station(check_cod, verbose=False)
                # Si la estación no está en mapa-estaciones-2, intentar igualmente —
                # puede estar disponible en otro sistema (export.php bruteforce o datos locales).
                # NO abortar aquí: dejar que el job intente la descarga de todas formas.
            except Exception:
                portal_params = None  # Red no disponible — intentar igualmente

        # --- Lanzar descarga en hilo de fondo (no bloquea el servidor) ---
        job_id = uuid.uuid4().hex[:10]

        def _log(msg):
            """Agrega un mensaje de progreso al job (visible en el frontend)."""
            with _jobs_lock:
                job = _download_jobs.get(job_id)
                if job and 'logs' in job:
                    job['logs'].append(str(msg))

        def _bg_download(pp=portal_params):
            import sys, threading as _threading

            # Redirigir stdout para capturar los prints de app.py (ej: "+ 202401: 30 registros")
            # Solo captura prints del hilo actual para evitar contaminación con otros hilos.
            _my_tid = _threading.get_ident()

            class _TeeStream:
                def __init__(self, orig):
                    self._orig = orig
                    self._buf = ''
                def write(self, s):
                    self._orig.write(s)
                    # Solo loguear si somos el hilo de descarga
                    if _threading.get_ident() != _my_tid:
                        return
                    self._buf += s
                    while '\n' in self._buf:
                        line, self._buf = self._buf.split('\n', 1)
                        line = line.strip()
                        if line:
                            _log(line)
                def flush(self):
                    self._orig.flush()

            orig_stdout = sys.stdout
            sys.stdout = _TeeStream(orig_stdout)
            try:
                p, e = _attempt_save(df, '', portal_params=pp)
                local_e = e
                if p is None:
                    try:
                        df_remote = scraper.get_stations(use_local=False)
                        p, remote_e = _attempt_save(df_remote, ' (remoto)', portal_params=pp)
                        if remote_e == 'station not found' and local_e:
                            e = local_e
                        elif remote_e and 'Sin datos' in (remote_e or ''):
                            e = remote_e
                        else:
                            e = remote_e or local_e
                    except Exception as rexc:
                        e = local_e or str(rexc)

                if p:
                    file_name = Path(p).name
                    result = {'saved': True, 'path': str(p), 'file': file_name,
                              'download_url': f'/api/download_output?file={file_name}'}
                else:
                    result = {'saved': False, 'error': e or 'Sin datos disponibles en SENAMHI'}

                with _jobs_lock:
                    _download_jobs[job_id] = {'status': 'done', 'result': result,
                                               'logs': _download_jobs.get(job_id, {}).get('logs', [])}
            except Exception as ex:
                with _jobs_lock:
                    _download_jobs[job_id] = {'status': 'failed', 'error': str(ex), 'result': None,
                                               'logs': _download_jobs.get(job_id, {}).get('logs', [])}
            finally:
                sys.stdout = orig_stdout

        with _jobs_lock:
            _download_jobs[job_id] = {'status': 'pending', 'result': None, 'error': None, 'logs': []}

        threading.Thread(target=_bg_download, daemon=True).start()
        return jsonify({'job_id': job_id, 'status': 'pending'})

    except Exception as e:
        return jsonify({'saved': False, 'error': f'error saving station: {str(e)}'}), 500


@app.route('/api/job_status/<job_id>')
def api_job_status(job_id):
    """Devuelve el estado de un trabajo de descarga en segundo plano."""
    with _jobs_lock:
        job = _download_jobs.get(job_id)
    if job is None:
        return jsonify({'status': 'not_found'}), 404
    return jsonify(job)


# ── SESIONES AUTENTICADAS SENAMHI HISTÓRICO ──────────────────────────────────
# Almacena objetos requests.Session con cookies activas, indexados por session_id UUID.
_hist_sessions = {}   # session_id -> {'session': requests.Session, 'email': str}
_hist_sessions_lock = threading.Lock()

HIST_BASE = 'https://www.senamhi.gob.pe/site/descarga-datos/descarga/'
CAPTCHA_URL = 'https://www.senamhi.gob.pe/include/captcha/graf_gd_genera_captcha.php?size=little'
LOGIN_URL   = HIST_BASE + '_php_login.php'


def _new_hist_session():
    """Crea una requests.Session con headers de navegador para el portal histórico."""
    sess = requests.Session()
    sess.headers.update({
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                       'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-PE,es;q=0.9,en;q=0.8',
        'Referer': 'https://www.senamhi.gob.pe/site/descarga-datos/',
    })
    return sess


@app.route('/api/hist_session', methods=['POST'])
def api_hist_session():
    """
    Registra un PHPSESSID existente del navegador del usuario.
    Verifica que la sesión sea válida intentando acceder a una página protegida.
    """
    import uuid as _uuid
    data = request.json or {}
    phpsessid = data.get('phpsessid', '').strip()
    if not phpsessid:
        return jsonify({'ok': False, 'error': 'PHPSESSID vacío'}), 400

    sess = _new_hist_session()
    sess.cookies.set('PHPSESSID', phpsessid, domain='www.senamhi.gob.pe', path='/')
    sess.headers.update({'Referer': 'https://www.senamhi.gob.pe/site/descarga-datos/'})

    try:
        # Verificar que la sesión es válida accediendo a la página de descarga
        r = sess.get(HIST_BASE + '?p=descarga&cod=000708', timeout=15)
        # Si redirige a login, la sesión no es válida
        if '?p=login' in r.url or 'frmlogin' in r.text:
            return jsonify({'ok': False, 'error': 'Sesión expirada o inválida — vuelve a iniciar sesión en el portal SENAMHI'})
        # Sesión válida
        session_id = _uuid.uuid4().hex
        with _hist_sessions_lock:
            _hist_sessions[session_id] = {'session': sess, 'email': phpsessid[:8], 'logged_in': True}
        return jsonify({'ok': True, 'session_id': session_id})
    except Exception as ex:
        return jsonify({'ok': False, 'error': str(ex)}), 502


@app.route('/api/hist_captcha')
def api_hist_captcha():
    """
    Obtiene una imagen captcha nueva del portal histórico SENAMHI.
    Devuelve: {captcha_b64, session_id, phpsessid}
    El frontend muestra la imagen y envía session_id + texto al endpoint de login.
    """
    import base64, uuid as _uuid
    sess = _new_hist_session()
    try:
        r = sess.get(CAPTCHA_URL, timeout=15)
        if r.status_code != 200:
            return jsonify({'ok': False, 'error': f'HTTP {r.status_code}'}), 502
        captcha_b64 = base64.b64encode(r.content).decode()
        phpsessid = sess.cookies.get('PHPSESSID', '')
        session_id = str(_uuid.uuid4().hex)
        with _hist_sessions_lock:
            _hist_sessions[session_id] = {'session': sess, 'email': '', 'logged_in': False}
        return jsonify({'ok': True, 'captcha_b64': captcha_b64, 'session_id': session_id})
    except Exception as ex:
        return jsonify({'ok': False, 'error': str(ex)}), 502


@app.route('/api/hist_login', methods=['POST'])
def api_hist_login():
    """
    Hace login al portal histórico usando las credenciales + captcha del usuario.
    Body JSON: {session_id, email, password, captcha}
    Devuelve: {ok, error?}
    """
    data = request.json or {}
    session_id = data.get('session_id', '')
    email      = data.get('email', '').strip()
    password   = data.get('password', '').strip()
    captcha    = data.get('captcha', '').strip()  # NO convertir — SENAMHI valida con mayúsculas/minúsculas exactas

    with _hist_sessions_lock:
        entry = _hist_sessions.get(session_id)
    if not entry:
        return jsonify({'ok': False, 'error': 'Sesión no encontrada — recarga el captcha'}), 400

    sess = entry['session']
    sess.headers.update({'Referer': HIST_BASE + '?p=login'})
    try:
        r = sess.post(LOGIN_URL, data={'mail': email, 'pass': password, 'captcha': captcha}, timeout=15)
        text = r.text.strip()
        if text.startswith('Error:'):
            return jsonify({'ok': False, 'error': text.replace('Error:', '').strip()})
        # Login exitoso
        with _hist_sessions_lock:
            _hist_sessions[session_id]['email'] = email
            _hist_sessions[session_id]['logged_in'] = True
        return jsonify({'ok': True, 'message': 'Sesión iniciada correctamente'})
    except Exception as ex:
        return jsonify({'ok': False, 'error': str(ex)}), 502


@app.route('/api/hist_download', methods=['POST'])
def api_hist_download():
    """
    Paso 1: Obtiene la página de descarga y devuelve el captcha al frontend.
    Body JSON: {session_id, cod_estacion, station_name, from_date, to_date}
    """
    import base64 as _b64, re as _re
    data = request.json or {}
    session_id   = data.get('session_id', '')
    cod          = str(data.get('cod_estacion', '')).strip()
    station_name = data.get('station_name', 'estacion').strip()
    from_date    = data.get('from_date', '')   # YYYY-MM-DD
    to_date      = data.get('to_date', '')

    with _hist_sessions_lock:
        entry = _hist_sessions.get(session_id)
    if not entry or not entry.get('logged_in'):
        return jsonify({'ok': False, 'error': 'No has iniciado sesión'}), 401

    sess = entry['session']
    try:
        # Visitar la página de descarga (autenticada)
        get_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-PE,es;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Upgrade-Insecure-Requests': '1',
        }
        r_form = sess.get(HIST_BASE + f'?p=descarga&cod={cod}', headers=get_headers, timeout=20)
        if '?p=login' in r_form.url or 'frmlogin' in r_form.text:
            with _hist_sessions_lock:
                _hist_sessions[session_id]['logged_in'] = False
            return jsonify({'ok': False, 'error': 'Sesión expirada — pega tu PHPSESSID de nuevo'}), 401

        html = r_form.text

        # ── Extraer todos los campos del formulario ────────────────────────────
        form_fields = {}

        # inputs (hidden, text, etc.) — excluimos captcha y submit
        for m in _re.finditer(r'<input([^>]*)>', html, _re.I):
            attrs = m.group(1)
            typ_m  = _re.search(r'type=["\']([^"\']+)["\']', attrs, _re.I)
            typ    = typ_m.group(1).lower() if typ_m else 'text'
            if typ in ('submit', 'button', 'image', 'reset'):
                continue
            name_m  = _re.search(r'name=["\']([^"\']+)["\']', attrs, _re.I)
            value_m = _re.search(r'value=["\']([^"\']*)["\']', attrs, _re.I)
            if name_m:
                name = name_m.group(1)
                val  = value_m.group(1) if value_m else ''
                if name.lower() in ('captcha', 'aceptar', 'btndescargar'):
                    continue  # los manejamos explícitamente en paso 2
                form_fields[name] = val

        # selects — tomar la primera option seleccionada o la primera opción
        for sm in _re.finditer(r'<select([^>]*)>(.*?)</select>', html, _re.I | _re.S):
            s_attrs   = sm.group(1)
            s_inner   = sm.group(2)
            name_m    = _re.search(r'name=["\']([^"\']+)["\']', s_attrs, _re.I)
            if not name_m:
                continue
            name = name_m.group(1)
            # buscar option selected
            sel_m = _re.search(r'<option[^>]+selected[^>]*value=["\']([^"\']*)["\']', s_inner, _re.I)
            if not sel_m:
                sel_m = _re.search(r'<option[^>]+value=["\']([^"\']*)["\']', s_inner, _re.I)
            form_fields[name] = sel_m.group(1) if sel_m else ''

        # Asegurar cod de estación
        if 'cod' not in form_fields and cod:
            form_fields['cod'] = cod

        # ── Mapear fechas del frontend a los campos del formulario ─────────────
        # Los nombres comunes en SENAMHI: ano_ini/mes_ini/ano_fin/mes_fin
        if from_date:
            parts = from_date.split('-')
            if len(parts) >= 2:
                ano_ini, mes_ini = parts[0], parts[1]
                for k in list(form_fields.keys()):
                    kl = k.lower()
                    if 'ano' in kl and ('ini' in kl or 'start' in kl or 'desde' in kl or 'in' in kl):
                        form_fields[k] = ano_ini
                    elif 'mes' in kl and ('ini' in kl or 'start' in kl or 'desde' in kl or 'in' in kl):
                        form_fields[k] = mes_ini
                    elif 'year' in kl and ('ini' in kl or 'from' in kl):
                        form_fields[k] = ano_ini
                # Forzar si no encontramos
                if not any('ano' in k.lower() and 'ini' in k.lower() for k in form_fields):
                    form_fields.setdefault('ano_ini', ano_ini)
                    form_fields.setdefault('mes_ini', mes_ini)

        if to_date:
            parts = to_date.split('-')
            if len(parts) >= 2:
                ano_fin, mes_fin = parts[0], parts[1]
                for k in list(form_fields.keys()):
                    kl = k.lower()
                    if 'ano' in kl and ('fin' in kl or 'end' in kl or 'hasta' in kl):
                        form_fields[k] = ano_fin
                    elif 'mes' in kl and ('fin' in kl or 'end' in kl or 'hasta' in kl):
                        form_fields[k] = mes_fin
                if not any('ano' in k.lower() and 'fin' in k.lower() for k in form_fields):
                    form_fields.setdefault('ano_fin', ano_fin)
                    form_fields.setdefault('mes_fin', mes_fin)

        # ── URL del formulario (action) ────────────────────────────────────────
        from urllib.parse import urljoin as _urljoin
        action_m = _re.search(r'<form[^>]+action=["\']([^"\']*)["\']', html, _re.I)
        if action_m:
            form_action = _urljoin(r_form.url, action_m.group(1))
        else:
            form_action = _urljoin(r_form.url, '_txt_descargar.php')

        # ── Captcha: usar la URL exacta embebida en la página ─────────────────
        captcha_src_m = _re.search(r'<img[^>]+src=["\']([^"\']*captcha[^"\']*)["\']', html, _re.I)
        if captcha_src_m:
            captcha_url = _urljoin(r_form.url, captcha_src_m.group(1))
        else:
            captcha_url = 'https://www.senamhi.gob.pe/include/captcha/graf_gd_genera_captcha.php?size=little'

        print(f'[hist_download] form_action={form_action}')
        print(f'[hist_download] captcha_url={captcha_url}')
        print(f'[hist_download] form_fields={form_fields}')

        cap_r = sess.get(captcha_url, headers={
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Referer': HIST_BASE + f'?p=descarga&cod={cod}',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        }, timeout=10)
        captcha_b64 = _b64.b64encode(cap_r.content).decode()

        # Guardar todo en la sesión para el paso 2
        with _hist_sessions_lock:
            _hist_sessions[session_id]['dl_cod']          = cod
            _hist_sessions[session_id]['dl_name']         = station_name
            _hist_sessions[session_id]['dl_form_fields']  = form_fields
            _hist_sessions[session_id]['dl_captcha_url']  = captcha_url
            _hist_sessions[session_id]['dl_form_action']  = form_action
            _hist_sessions[session_id]['dl_from_date']    = from_date
            _hist_sessions[session_id]['dl_to_date']      = to_date

        return jsonify({'ok': True, 'needs_captcha': True, 'captcha_b64': captcha_b64,
                        'form_fields': form_fields,   # para debug en frontend
                        'user_name': _re.search(r'Usuario.*?:\s*(.*?)</td>', html, _re.S).group(1).strip() if 'Usuario' in html else ''})
    except Exception as ex:
        return jsonify({'ok': False, 'error': str(ex)}), 502


@app.route('/api/hist_do_download', methods=['POST'])
def api_hist_do_download():
    """
    Paso 2: Envía el captcha y descarga el archivo .txt.
    Body JSON: {session_id, captcha}
    """
    import re as _re
    data = request.json or {}
    session_id = data.get('session_id', '')
    captcha    = data.get('captcha', '').strip()

    with _hist_sessions_lock:
        entry = _hist_sessions.get(session_id)
    if not entry or not entry.get('logged_in'):
        return jsonify({'ok': False, 'error': 'Sesión no encontrada'}), 401

    sess         = entry['session']
    cod          = entry.get('dl_cod', '')
    station_name = entry.get('dl_name', 'estacion')
    form_fields  = entry.get('dl_form_fields', {})
    captcha_url  = entry.get('dl_captcha_url', 'https://www.senamhi.gob.pe/include/captcha/graf_gd_genera_captcha.php?size=little')
    form_action  = entry.get('dl_form_action', HIST_BASE + '_txt_descargar.php')

    try:
        # Construir el POST con los campos hidden + captcha + checkboxes
        post_data = dict(form_fields)
        post_data['captcha']      = captcha
        post_data['aceptar']      = 'on'
        post_data['btnDescargar'] = 'Descargar'

        print(f'[hist_do_download] POST to={form_action}  data={post_data}')
        post_headers = {
            'Referer': HIST_BASE + f'?p=descarga&cod={cod}',
            'Origin': 'https://www.senamhi.gob.pe',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'es-PE,es;q=0.9',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        }
        r_dl = sess.post(form_action,
            data=post_data,
            headers=post_headers,
            allow_redirects=True,
            timeout=30)

        content_disp = r_dl.headers.get('Content-Disposition', '')
        content_type = r_dl.headers.get('Content-Type', '')
        txt = r_dl.text.strip()

        # Log siempre para debug
        print(f'[hist_do_download] status={r_dl.status_code} content-type={content_type!r} content-disp={content_disp!r}')
        print(f'[hist_do_download] first200={txt[:200]!r}')

        # Verificar si es un archivo de datos:
        # 1) Content-Disposition con "attachment"
        # 2) Cualquier línea que empiece con un año (1900-2100)
        # 3) Content-Type text/plain (no html)
        lines = [l.strip() for l in txt.splitlines() if l.strip()]
        has_year_line = any(
            l.split()[0].isdigit() and 1900 <= int(l.split()[0]) <= 2100
            for l in lines if l.split()
        )
        is_html = txt.lower().startswith('<!') or '<html' in txt.lower()[:200]
        is_data = ('attachment' in content_disp or
                   has_year_line or
                   ('text/plain' in content_type and not is_html and len(txt) > 50))

        if is_data:
            import re as _re2
            mod = scraper or _load_scraper_module()

            # Parsear el txt con parse_hist_txt
            df = None
            if mod and hasattr(mod, 'parse_hist_txt'):
                try:
                    df = mod.parse_hist_txt(txt)
                except Exception as _e:
                    print(f'[hist_do_download] parse_hist_txt error: {_e}')

            # Filtrar al rango de fechas solicitado
            from_date = entry.get('dl_from_date', '')
            to_date   = entry.get('dl_to_date', '')
            if df is not None and not df.empty and 'fecha' in df.columns:
                if from_date:
                    df = df[df['fecha'] >= pd.Timestamp(from_date)]
                if to_date:
                    df = df[df['fecha'] <= pd.Timestamp(to_date)]

            output_dir = _get_data_dir() / 'outputs'
            output_dir.mkdir(parents=True, exist_ok=True)
            safe_name = _re2.sub(r'[^\w]', '_', station_name)

            if df is not None and not df.empty:
                # Guardar como Excel
                xl_name = f'hist_{safe_name}_{cod}_{from_date or "all"}_{to_date or "all"}.xlsx'
                xl_path = output_dir / xl_name
                # Reordenar columnas
                cols = [c for c in ['fecha','anio','mes','dia','precip_mm','tmax_c','tmin_c'] if c in df.columns]
                df[cols].to_excel(xl_path, index=False)
                n_rows = len(df)
                print(f'[hist_do_download] OK - {n_rows} filas -> {xl_path.name}')
                return jsonify({'ok': True, 'file': xl_path.name, 'rows': n_rows,
                                'download_url': f'/api/download_output?file={xl_path.name}'})
            else:
                # Guardar el txt crudo si no se pudo parsear
                fname_match = _re2.search(r'filename=([^\s;]+)', content_disp)
                fname = fname_match.group(1).strip('"') if fname_match else f'qc{cod}.txt'
                out_path = output_dir / f'hist_{safe_name}_{fname}'
                out_path.write_bytes(r_dl.content)
                n_rows = len([l for l in txt.splitlines() if l.strip() and l.split() and l.split()[0].isdigit()])
                print(f'[hist_do_download] OK (txt crudo) - {n_rows} filas -> {out_path.name}')
                return jsonify({'ok': True, 'file': out_path.name, 'rows': n_rows,
                                'download_url': f'/api/download_output?file={out_path.name}'})

        # No es datos — captcha incorrecto u otro error del portal
        import base64 as _b64
        print(f'[hist_do_download] NO ES DATO - probablemente captcha incorrecto o no hay archivo')
        # Obtener nuevo captcha usando la misma URL que la página
        cap_r = sess.get(captcha_url, headers={
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Referer': HIST_BASE + f'?p=descarga&cod={cod}',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        }, timeout=10)
        captcha_b64 = _b64.b64encode(cap_r.content).decode()
        return jsonify({'ok': False, 'needs_captcha': True, 'captcha_b64': captcha_b64,
                        'error': 'Código de seguridad incorrecto — intenta de nuevo',
                        'debug': txt[:300]})
    except Exception as ex:
        return jsonify({'ok': False, 'error': str(ex)}), 502


@app.route('/api/import_hist_txt', methods=['POST'])
def api_import_hist_txt():
    """
    Recibe un archivo .txt del portal histórico SENAMHI (qcNNNNNNNN.txt),
    lo parsea y lo guarda como Excel en DATA/outputs/.
    Formato del .txt: líneas con Año Mes Día Precip Tmax Tmin separados por espacio.
    """
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent))

    if 'file' not in request.files:
        return jsonify({'ok': False, 'error': 'No se recibió archivo'}), 400

    f = request.files['file']
    fname = f.filename or 'datos.txt'
    station_name = request.form.get('station_name', '') or Path(fname).stem

    txt_content = f.read().decode('utf-8', errors='replace')
    if not txt_content.strip():
        return jsonify({'ok': False, 'error': 'El archivo está vacío'}), 400

    # Parsear el .txt
    lines = [l.strip() for l in txt_content.strip().splitlines() if l.strip()]
    rows = []
    for line in lines:
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            anio = int(parts[0])
            mes  = int(parts[1])
            dia  = int(parts[2]) if len(parts) > 2 else 1
        except ValueError:
            continue
        try:
            fecha = f'{anio:04d}-{mes:02d}-{dia:02d}'
            row = {'Fecha': fecha, 'Año': anio, 'Mes': mes, 'Día': dia}
            cols = ['Precipitacion_mm', 'Tmax_C', 'Tmin_C', 'Hmedia_pct', 'Vviento_ms']
            for i, col in enumerate(cols):
                if i + 3 < len(parts):
                    val = float(parts[i + 3])
                    row[col] = None if val <= -99 else val
            rows.append(row)
        except Exception:
            continue

    if not rows:
        return jsonify({'ok': False, 'error': 'No se encontraron datos válidos en el archivo'}), 400

    import pandas as _pd
    df = _pd.DataFrame(rows)

    # Guardar como Excel
    output_dir = Path('g:/1_PROYECTOS/WEBSCRAPING/Web_Scraping_SENAMHI_/DATA/outputs')
    output_dir.mkdir(parents=True, exist_ok=True)
    import re as _re
    safe_name = _re.sub(r'[^\w]', '_', station_name)
    stem = Path(fname).stem
    out_path = output_dir / f'hist_{safe_name}_{stem}.xlsx'
    df.to_excel(out_path, index=False)

    return jsonify({'ok': True, 'rows': len(df), 'file': out_path.name,
                    'download_url': f'/api/download_output?file={out_path.name}'})


@app.route('/api/process_all', methods=['POST'])
def api_process_all():
    data = request.json or {}
    from_date = data.get('from_date', '2024-01-01')
    to_date = data.get('to_date', '2024-12-31')
    limit = data.get('limit')
    use_local = data.get('use_local', True)
    # Asegurar que el scraper esté cargado en tiempo de petición
    global scraper
    if scraper is None:
        scraper = _load_scraper_module()
        if scraper is None:
            return jsonify({'saved_files': []})

    df = scraper.get_stations(use_local=use_local)
    saved = scraper.process_all_stations(df, from_date, to_date, limit=limit)
    return jsonify({'saved_files': [str(p) for p in saved]})


@app.route('/api/save', methods=['GET'])
def api_save_get():
    # Quick GET wrapper to request saving by station name/code from query params
    station = request.args.get('station') or request.args.get('station_name')
    code = request.args.get('code')
    from_date = request.args.get('from_date') or request.args.get('from') or '2015-01-01'
    to_date = request.args.get('to_date') or request.args.get('to')
    if not to_date:
        from datetime import datetime, timedelta
        to_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    use_local = request.args.get('use_local', 'true').lower() == 'true'
    hint_ico = request.args.get('ico')
    hint_tipo = request.args.get('tipo')

    # Intentar recargar scraper si no está disponible
    global scraper
    if scraper is None:
        scraper = _load_scraper_module()
        if scraper is None:
            return jsonify({'saved': False, 'error': 'scraper not available'}), 500
    try:
        df = scraper.get_stations(use_local=use_local)
    except Exception as e:
        return jsonify({'saved': False, 'error': f'error getting stations: {str(e)}'}), 500

    try:
        df_work = df.copy().reset_index(drop=True)

        # Localizar por codigo o nombre
        iloc_idx = None
        if code:
            mask = df_work['cod'].astype(str).str.strip().str.upper() == str(code).strip().upper()
            if mask.any():
                iloc_idx = df_work.index.get_loc(df_work[mask].index[0])
            else:
                mask2 = df_work['cod'].astype(str).fillna('').str.contains(str(code), case=False, na=False)
                if mask2.any():
                    iloc_idx = df_work.index.get_loc(df_work[mask2].index[0])

        if iloc_idx is None and station:
            mask = df_work['estacion'].astype(str).str.contains(str(station), case=False, na=False)
            if mask.any():
                iloc_idx = df_work.index.get_loc(df_work[mask].index[0])

        if iloc_idx is None:
            return jsonify({'saved': False, 'error': 'station not found or no data saved'}), 404

        # Inyectar hints si estan disponibles
        if hint_ico and str(hint_ico).strip().upper() in ('M', 'H'):
            df_work.at[iloc_idx, 'ico'] = str(hint_ico).strip().upper()
        elif hint_tipo:
            tipo_low = str(hint_tipo).lower()
            if 'auto' in tipo_low and 'AUTOMATICA' in df_work.columns:
                val = df_work.at[iloc_idx, 'AUTOMATICA']
                if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
                    df_work.at[iloc_idx, 'AUTOMATICA'] = 'EMA'
            elif 'conv' in tipo_low and 'CONVENCIONAL' in df_work.columns:
                val = df_work.at[iloc_idx, 'CONVENCIONAL']
                if pd.isna(val) or str(val).strip() in ('', 'nan', 'None'):
                    df_work.at[iloc_idx, 'CONVENCIONAL'] = 'CO'

        path = scraper.save_station_by_index(df_work, iloc_idx, from_date, to_date)
        if path:
            # Validar que el archivo guardado contiene datos reales
            if not _saved_file_has_data(path):
                return jsonify({'saved': False, 'error': 'no data saved or marker file created'}), 404
            file_name = Path(path).name
            return jsonify({'saved': True, 'path': str(path), 'file': file_name, 'download_url': f"/api/download_output?file={file_name}"})

        return jsonify({'saved': False, 'error': 'station not found or no data saved'}), 404
    except Exception as e:
        return jsonify({'saved': False, 'error': f'error saving station: {str(e)}'}), 500


@app.route('/api/download_output')
def api_download_output():
    # Serve a file from DATA/outputs as attachment. Use `file` query param with filename.
    fname = (request.args.get('file') or '').strip()
    base = _find_data_dir() / 'outputs'
    if not fname:
        return jsonify({'error': 'missing file parameter'}), 400
    # prevent path traversal
    target = (base / fname).resolve()
    try:
        if not target.exists() or not str(target).startswith(str(base.resolve())):
            return jsonify({'error': 'file not found'}), 404
        return send_from_directory(str(base), fname, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
