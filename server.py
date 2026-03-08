from flask import Flask, send_from_directory, jsonify, request
from pathlib import Path
import random
import shapefile
import json
import pandas as pd
import math
import time

app = Flask(__name__, static_folder='.')

try:
    import app as scraper
except Exception as e:
    scraper = None


@app.route('/')
def index():
    return send_from_directory('.', 'dashboard_hidrometeo.html')


@app.route('/api/stations')
def api_stations():
    use_local = request.args.get('use_local', 'true').lower() == 'true'
    # Prefer reading local maestra directly (ensures coordinates present).
    base = Path(__file__).resolve().parents[0] / 'DATA'
    maestra = base / 'Maestra_de_estaciones_Senamhi.xlsx'
    df = None
    if use_local and maestra.exists():
        try:
            df = pd.read_excel(maestra)
        except Exception:
            df = None

    # fallback to scraper if local read failed and scraper available
    if df is None and scraper is not None:
        df = scraper.get_stations(use_local=use_local)

    if df is None:
        return jsonify([])
    stations = []
    # Priorizar columnas exactas de la maestra (mayúsculas/español)
    cols_upper = {c.upper(): c for c in df.columns}

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
            tipo_val = None
            try:
                if auto_col and str(row.get(auto_col) or '').strip() not in ('', '0', 'False', 'FALSE'):
                    tipo_val = 'Automática'
                elif conv_col and str(row.get(conv_col) or '').strip() not in ('', '0', 'False', 'FALSE'):
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
    base = Path(__file__).resolve().parents[0] / 'DATA'

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


@app.route('/api/outputs')
def api_outputs():
    base = Path(__file__).resolve().parents[0] / 'DATA' / 'outputs'
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
    base = Path(__file__).resolve().parents[0] / 'DATA' / 'outputs'
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
    # Campos opcionales que el dashboard puede enviar para mejorar la resolucion
    # de parametros del portal (t_e e estado en export.php)
    hint_tipo = data.get('tipo')          # 'Convencional' o 'Automatica'
    hint_estado_op = data.get('estado_operativo')  # 'OPERATIVA', 'INACTIVA', etc.
    hint_ico = data.get('ico')            # 'M' o 'H' si el cliente lo conoce

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
        """Devuelve el iloc-index (entero) de la primera fila que coincide."""
        # Por codigo exacto primero
        if by_code:
            mask = df_local['cod'].astype(str).str.strip().str.upper() == str(by_code).strip().upper()
            if mask.any():
                return df_local.index.get_loc(df_local[mask].index[0])
            # Coincidencia parcial como fallback
            mask2 = df_local['cod'].astype(str).fillna('').str.contains(str(by_code), case=False, na=False)
            if mask2.any():
                return df_local.index.get_loc(df_local[mask2].index[0])
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

    def _attempt_save(df_src, use_remote_label=''):
        """Intenta localizar y guardar. Devuelve (path, error_str)."""
        df_work = df_src.copy().reset_index(drop=True)
        iloc_idx = _find_row_index(df_work, by_code=code, by_name=name)
        if iloc_idx is None:
            return None, 'station not found'
        df_work = _patch_row_with_hints(df_work, iloc_idx)
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
                    return None, f'no_data{use_remote_label}'
                # Si existe una columna 'status', comprobar si TODOS los valores
                # indican no_data (evita falsos positivos cuando la tabla real
                # contiene una columna llamada 'status').
                if 'status' in df_check.columns:
                    try:
                        statuses = df_check['status'].astype(str).str.lower()
                        if statuses.str.contains('no_data', na=False).all():
                            return None, f'no_data{use_remote_label}'
                    except Exception:
                        # Si algo falla en la comprobación, no bloquear el guardado
                        pass
            except Exception:
                pass
            return path, None
        return None, f'no data saved{use_remote_label}'

    try:
        # --- Intento 1: lista local (rapido, no requiere red para la lista) ---
        path, err = _attempt_save(df, '')

        # --- Intento 2: lista remota (tiene ico y estado reales del portal) ---
        # Se ejecuta si el intento local no dio datos reales. Identico al metodo
        # que usa run_orcopampa_full.py, que es el que funciono historicamente.
        if path is None:
            try:
                df_remote = scraper.get_stations(use_local=False)
                path, err = _attempt_save(df_remote, ' (remoto)')
            except Exception as remote_exc:
                err = err or str(remote_exc)

        if path:
            file_name = Path(path).name
            return jsonify({
                'saved': True,
                'path': str(path),
                'file': file_name,
                'download_url': f"/api/download_output?file={file_name}"
            })

        return jsonify({'saved': False, 'error': err or 'no data saved or station not found'}), 404
    except Exception as e:
        return jsonify({'saved': False, 'error': f'error saving station: {str(e)}'}), 500


@app.route('/api/process_all', methods=['POST'])
def api_process_all():
    data = request.json or {}
    from_date = data.get('from_date', '2024-01-01')
    to_date = data.get('to_date', '2024-12-31')
    limit = data.get('limit')
    use_local = data.get('use_local', True)
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
    base = Path(__file__).resolve().parents[0] / 'DATA' / 'outputs'
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
    app.run(host='0.0.0.0', port=5000, debug=True)
