import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
from warnings import filterwarnings
from datetime import datetime
import re
import requests
from bs4 import BeautifulSoup
import os
from pathlib import Path

filterwarnings('ignore')

# ========================================
# FUNCIONES PRINCIPALES
# ========================================

def get_stations_senamhi():
    """
    Extrae TODAS las estaciones del servidor SENAMHI
    Returns: DataFrame con todas las estaciones
    """
    print("🔍 Extrayendo estaciones del servidor SENAMHI...")

    link = "https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/"
    response = requests.get(link)
    stn_senamhi = BeautifulSoup(response.text, 'html.parser')

    stn_senamhi2 = re.split(r'nom', str(stn_senamhi))[1:]
    data_stn = []

    for i in range(len(stn_senamhi2)):
        x = stn_senamhi2[i].replace('"', '').replace(': ', ":").replace(',\n', "").replace('\}\{', "")
        data_estaciones = x.split(",")

        try:
            stn_name = data_estaciones[0].replace(":", "")
            cat = data_estaciones[1].replace("cate:", "")
            lat = data_estaciones[2].replace("lat:", "")
            lon = data_estaciones[3].replace("lon:", "")
            ico = data_estaciones[4].replace(" ico:", "")
            cod = data_estaciones[5].replace(" cod:", "") if len(data_estaciones) > 5 and data_estaciones[5][:5] == " cod:" else None
            cod_old = data_estaciones[6].replace("cod_old:", "") if len(data_estaciones) > 6 and data_estaciones[6][:8] == "cod_old:" else None
            estado_value = data_estaciones[7] if len(data_estaciones) > 7 else (data_estaciones[6] if len(data_estaciones) > 6 else "")
            estado = estado_value.replace("}{", "").replace(" estado:", "") if estado_value[:8] == " estado:" else "ACTUALMENTE"

            data_stn.append(pd.DataFrame({
                'estacion': stn_name,
                'categoria': cat,
                'lat': lat,
                'lon': lon,
                'ico': ico,
                'cod': cod,
                'cod_old': cod_old,
                'estado': estado
            }, index=[0]))
        except:
            continue

    df_stns = pd.concat(data_stn, ignore_index=True)

    # Convertir coordenadas a numérico
    df_stns['lat'] = pd.to_numeric(df_stns['lat'], errors='coerce')
    df_stns['lon'] = pd.to_numeric(df_stns['lon'], errors='coerce')
    df_stns = df_stns.dropna(subset=['lat', 'lon'])

    print(f"✓ {len(df_stns)} estaciones extraídas de SENAMHI")
    return df_stns


def search_stations(df_stations, name=None, categoria=None, estado=None):
    """
    Busca estaciones por nombre, categoría o estado

    Args:
        df_stations: DataFrame de estaciones
        name: Texto a buscar en nombre de estación
        categoria: Categoría exacta
        estado: Estado exacto

    Returns: DataFrame filtrado
    """
    df = df_stations.copy()

    if name:
        df = df[df['estacion'].str.contains(name, case=False, na=False)]

    if categoria:
        df = df[df['categoria'] == categoria]

    if estado:
        df = df[df['estado'] == estado]

    return df


def _build_senamhi_session():
    """
    Crea una sesión requests con headers realistas y visita la pagina principal
    de SENAMHI para obtener cookies de sesion. Esto es necesario porque export.php
    requiere que la sesion PHP este inicializada desde la pagina del mapa.
    """
    import time as _time
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    session.mount('http://', HTTPAdapter(max_retries=retries))

    # Headers que imitan un navegador real — el portal rechaza requests sin User-Agent
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/124.0.0.0 Safari/537.36'
        ),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'es-PE,es;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/',
    })

    # Visitar la pagina principal para obtener cookies de sesion PHP
    try:
        session.get(
            'https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/',
            timeout=20
        )
        _time.sleep(0.5)
    except Exception:
        pass  # Si falla, continuar de todas formas

    return session


def _parse_export_html(html_text):
    """
    Parsea la respuesta HTML de export.php y devuelve un DataFrame o None.

    El portal SENAMHI devuelve una pagina con al menos 2 tablas HTML:
    - tabla[0]: cabecera/info de la estacion
    - tabla[1]: datos meteorologicos con fila de cabecera duplicada

    NOTA IMPORTANTE: export.php puede incluir un error PHP de include
    (_functionTest.php Failed to open stream) al inicio del HTML, pero
    IGUALMENTE devuelve las tablas de datos. No se debe abortar por ese
    error — se intenta parsear las tablas de todas formas.

    Solo se retorna None si la pagina explicitamente indica sin datos
    O si no hay ninguna tabla parseable con datos reales.
    """
    # Detectar respuesta que explicitamente indica sin datos
    if 'No se puede mostrar los datos' in html_text:
        return None

    # CORRECCION: el error '_functionTest.php Failed to open stream' NO impide
    # que export.php devuelva tablas con datos. Se registra pero NO se aborta.
    _has_php_error = ('_functionTest.php' in html_text and 'Failed to open stream' in html_text)

    try:
        tables = pd.read_html(html_text, flavor='lxml')
    except Exception:
        try:
            tables = pd.read_html(html_text)
        except ValueError:
            return None
        except Exception:
            return None

    if len(tables) < 2:
        # Si solo hay 1 tabla, intentar usarla como datos directamente
        if len(tables) == 1:
            df_candidate = tables[0].copy()
            if len(df_candidate) > 1:
                df_candidate.columns = df_candidate.iloc[0]
                df_candidate = df_candidate[1:].reset_index(drop=True)
                if len(df_candidate) > 0:
                    return df_candidate
        return None

    df_data = tables[1].copy()

    # La primera fila es cabecera repetida — usarla como nombres de columna
    df_data.columns = df_data.iloc[0]
    df_data = df_data[1:].reset_index(drop=True)

    # Eliminar filas que son repeticion del encabezado
    mask = df_data.iloc[:, 0].astype(str).str.strip().isin(['AÑO / MES / DÍA', 'ANO / MES / DIA', 'Fecha'])
    df_data = df_data[~mask].reset_index(drop=True)

    if len(df_data) == 0:
        return None

    return df_data


def _resolve_portal_params(row):
    """
    Construye las listas de candidatos (cod, ico, estado) para probar contra export.php.

    El portal SENAMHI export.php requiere:
      - t_e : 'M' (meteorologica) o 'H' (hidrologica)
      - estado : 'DIFERIDO', 'REAL' o 'AUTOMATICA'

    La Maestra local Excel tiene valores distintos:
      - ico: None (columna ausente en la Maestra)
      - estado: 'OPERATIVA', 'CLAUSURADA', 'SUSPENDIDA', 'PARCIALMENTE', NaN
      - CONVENCIONAL: codigo de instrumento si existe (CO, MAP, PLU, etc.)
      - AUTOMATICA: codigo de instrumento si existe (EMA, EHA, EAMA, etc.)

    Esta funcion genera todos los candidatos plausibles ordenados por probabilidad
    de exito, para que download_station_data_robust() pueda probarlos en orden.

    Devuelve: lista de dicts {cod, ico, estado_portal, cod_old}
    """
    import math as _math

    def _is_empty(v):
        if v is None:
            return True
        if isinstance(v, float) and _math.isnan(v):
            return True
        s = str(v).strip()
        return s in ('', 'nan', 'None', '0', 'False', 'FALSE')

    cod = str(row.get('cod') or '').strip()
    if not cod or cod in ('nan', 'None', ''):
        return []

    cod_old_raw = row.get('cod_old')
    try:
        cod_old = None if _is_empty(cod_old_raw) else str(cod_old_raw).strip()
    except Exception:
        cod_old = None

    # --- Determinar t_e (tipo de estacion para el portal) ---
    # Prioridad: campo ico del portal (solo disponible si se cargo desde SENAMHI remoto)
    # > columna AUTOMATICA no vacia => 'M' (mayoria de automaticas son met.)
    # > columna CONVENCIONAL no vacia => 'M'
    # > default 'M' (meteorologica es el tipo mas comun)
    ico_raw = row.get('ico')
    has_ico = not _is_empty(ico_raw)
    ico_portal = str(ico_raw).strip().upper() if has_ico else None

    has_auto = not _is_empty(row.get('AUTOMATICA'))
    has_conv = not _is_empty(row.get('CONVENCIONAL'))

    # --- Determinar estado_portal ---
    # La Maestra usa: OPERATIVA, CLAUSURADA, SUSPENDIDA, PARCIALMENTE, NaN
    # export.php espera: DIFERIDO, REAL, AUTOMATICA
    # Mapeamos segun el tipo de instrumento y el estado operativo de la Maestra:
    #   - Estacion con AUTOMATICA != NaN => probar 'AUTOMATICA' primero, luego 'REAL'
    #   - Estacion con CONVENCIONAL != NaN => probar 'DIFERIDO' primero, luego 'REAL'
    #   - Si el ico del portal ya dice 'M' o 'H', probar los tres estados en orden
    estado_maestra = str(row.get('estado') or '').strip().upper()

    # Construir lista de candidatos (cod, ico, estado_portal)
    candidates = []
    seen = set()

    def add(c, i, e):
        key = (c, i, e)
        if key not in seen:
            seen.add(key)
            candidates.append({'cod': c, 'ico': i, 'estado_portal': e, 'cod_old': cod_old})

    # Si ya tenemos ico del portal (lista remota), usarlo directamente con todos los estados
    if ico_portal in ('M', 'H'):
        if has_auto:
            add(cod, ico_portal, 'AUTOMATICA')
        add(cod, ico_portal, 'DIFERIDO')
        add(cod, ico_portal, 'REAL')
        add(cod, ico_portal, 'AUTOMATICA')
    else:
        # Lista local sin ico: deducir por tipo de instrumento
        if has_auto:
            # Estacion automatica
            add(cod, 'M', 'AUTOMATICA')
            add(cod, 'M', 'REAL')
            add(cod, 'H', 'AUTOMATICA')
            add(cod, 'H', 'REAL')
        if has_conv:
            # Estacion convencional
            add(cod, 'M', 'DIFERIDO')
            add(cod, 'M', 'REAL')
            add(cod, 'H', 'DIFERIDO')
            add(cod, 'H', 'REAL')
        # Si no sabemos nada, probar todas las combinaciones en orden de frecuencia
        if not has_auto and not has_conv:
            for ico_try in ('M', 'H'):
                for est_try in ('DIFERIDO', 'REAL', 'AUTOMATICA'):
                    add(cod, ico_try, est_try)

    # Tambien probar con cod_old si existe (algunas estaciones tienen codigos historicos)
    if cod_old:
        if ico_portal in ('M', 'H'):
            add(cod_old, ico_portal, 'DIFERIDO')
            add(cod_old, ico_portal, 'REAL')
            add(cod_old, ico_portal, 'AUTOMATICA')
        else:
            for ico_try in ('M', 'H'):
                for est_try in ('DIFERIDO', 'REAL', 'AUTOMATICA'):
                    add(cod_old, ico_try, est_try)

    return candidates


def _normalize_downloaded_df(df):
    """Normaliza un DataFrame descargado desde export.php antes de guardarlo.

    Reglas aplicadas:
    - Trim de strings.
    - Detecta columnas de precipitación por nombre y convierte marcadores 'T' a 0.0
      (traza) en esas columnas.
    - Reemplaza marcadores no numéricos ('T', 'T.', '-') por NaN en columnas
      que se puedan convertir a numéricas.
    - Intenta convertir columnas a numéricas cuando al menos 30% de valores
      pueden convertirse.
    """
    import pandas as _pd
    import numpy as _np

    df2 = df.copy()

    # Strip strings
    for col in df2.columns:
        try:
            df2[col] = df2[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        except Exception:
            continue

    # Detectar columnas de precipitación por nombre
    precip_cols = [c for c in df2.columns if 'precip' in str(c).lower() or 'precipit' in str(c).lower()]

    # Normalizar marcadores
    for col in df2.columns:
        if col in precip_cols:
            # 'T' -> 0 (traza) para precipitación
            df2[col] = df2[col].replace({'T': 0, 't': 0, 'T.': 0, 'T\n': 0})
        else:
            df2[col] = df2[col].replace({'T': _np.nan, 't': _np.nan, 'T.': _np.nan, 'T\n': _np.nan, '-': _np.nan})

    # Intentar convertir columnas a numéricas si tiene sentido
    for col in df2.columns:
        try:
            # reemplazar comas por puntos en representaciones numéricas
            series = df2[col].astype(str).str.replace(',', '.').replace({'nan': _pd.NA, 'None': _pd.NA})
            coerced = _pd.to_numeric(series, errors='coerce')
            non_null_ratio = coerced.notna().sum() / max(1, len(coerced))
            if non_null_ratio >= 0.3:
                df2[col] = coerced
        except Exception:
            continue

    return df2


def download_station_data(cod, ico, estado, cod_old, from_date, to_date, verbose=True):
    """
    Descarga datos historicos de una estacion desde el portal SENAMHI.

    Estrategia:
    1. Inicializar sesion HTTP visitando la pagina del mapa (obtiene cookies PHP).
    2. Iterar mes a mes construyendo la URL de export.php con los parametros correctos.
    3. Parsear la tabla HTML de respuesta.
    4. Si export.php devuelve error del servidor (pagina rota), reportar claramente.

    NOTA IMPORTANTE sobre el parametro `estado`:
    El portal export.php espera 'DIFERIDO', 'REAL' o 'AUTOMATICA' — NO los valores
    de la Maestra Excel ('OPERATIVA', 'CLAUSURADA', etc.). Si se pasa un valor
    no reconocido, el portal devuelve "No se puede mostrar los datos".
    Esta funcion acepta cualquier valor y lo normaliza internamente.

    Args:
        cod: Codigo de estacion (ej: '103043' o '472DE6A0')
        ico: Icono/tipo de estacion del portal ('M' o 'H'). Puede ser None.
        estado: Estado — puede ser del portal ('DIFERIDO','REAL','AUTOMATICA') o
                de la Maestra ('OPERATIVA','CLAUSURADA',etc.). Se normaliza.
        cod_old: Codigo antiguo (puede ser NaN/None)
        from_date: Fecha inicio (formato 'YYYY-MM-DD')
        to_date: Fecha fin (formato 'YYYY-MM-DD')
        verbose: Mostrar mensajes de progreso

    Returns: DataFrame con datos o None si no hay datos disponibles
    """
    import time as _time

    # Normalizar estado al formato que entiende export.php
    # Si ya es un valor del portal, usarlo; si es de la Maestra, mapear al mas probable
    ESTADOS_PORTAL = {'DIFERIDO', 'REAL', 'AUTOMATICA'}
    estado_str = str(estado).strip().upper() if estado else ''
    if estado_str in ESTADOS_PORTAL:
        estado_portal = estado_str
    else:
        # Valor de la Maestra u otro: usar DIFERIDO como default conservador
        estado_portal = 'DIFERIDO'

    # Normalizar ico
    ico_str = str(ico).strip().upper() if ico and str(ico).strip() not in ('', 'nan', 'None') else 'M'
    if ico_str not in ('M', 'H'):
        ico_str = 'M'

    df_history = []
    request_timeout = 45

    ts_date = pd.date_range(from_date, to_date, freq='MS')
    tsw_date = ts_date.strftime('%Y%m')

    if len(ts_date) == 0:
        if verbose:
            print(f"  ! Rango de fechas vacio: {from_date} -> {to_date}")
        return None

    # Inicializar sesion con cookies y headers correctos
    session = _build_senamhi_session()

    # Rastrear si el servidor esta devolviendo errores PHP para no seguir reintentando
    server_broken_count = 0
    MAX_BROKEN = 3  # Si 3 meses consecutivos fallan por error del servidor, abortar

    # Preparar cod_old
    try:
        cod_old_val = None if (cod_old is None or (isinstance(cod_old, float) and pd.isna(cod_old))) else str(cod_old)
    except Exception:
        cod_old_val = None

    for j, date in enumerate(ts_date):
        ym = tsw_date[j]

        # Construir URL con los parametros exactos que usa el portal
        # Nota: el campo 't_e' usa el valor 'ico' del portal ('M' o 'H'), NO la categoria
        base_url = 'https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/export.php'
        params = {
            'estaciones': str(cod),
            'CBOFiltro': ym,
            't_e': ico_str,
            'estado': estado_portal,
        }

        if cod_old_val:
            params['cod_old'] = cod_old_val

        # Construir URL manualmente para control total (evitar encoding doble)
        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        link = f"{base_url}?{param_str}"

        try:
            resp = session.get(link, timeout=request_timeout)

            if resp.status_code != 200:
                if verbose:
                    print(f"  ! HTTP {resp.status_code} en {ym} — omitiendo")
                server_broken_count += 1
                if server_broken_count >= MAX_BROKEN:
                    if verbose:
                        print(f"  ! El servidor responde con errores repetidos. Abortando descarga.")
                    break
                _time.sleep(1)
                continue

            # Detectar error PHP del servidor — SOLO registrar, NO abortar.
            # export.php devuelve tablas con datos incluso cuando el include falla.
            if '_functionTest.php' in resp.text and 'Failed to open stream' in resp.text:
                if verbose and j == 0:
                    print(
                        f"  ! AVISO: export.php tiene un error PHP de include en el servidor "
                        f"(archivo _functionTest.php faltante). Las tablas de datos pueden "
                        f"igualmente estar presentes — se intenta parsear de todas formas."
                    )

            server_broken_count = 0  # Reset si la respuesta fue valida (HTTP 200)

            df_data = _parse_export_html(resp.text)

            if df_data is not None and len(df_data) > 0:
                # Agregar columna de periodo para trazabilidad
                df_data['_periodo'] = ym
                df_history.append(df_data)
                if verbose:
                    print(f"  + {ym}: {len(df_data)} registros")
            else:
                if verbose:
                    print(f"  - {ym}: sin datos")

        except requests.exceptions.SSLError as e:
            if verbose:
                print(f"  ! SSL error en {ym}: {e}")
            _time.sleep(2)
            continue
        except requests.exceptions.ConnectionError as e:
            if verbose:
                print(f"  ! Connection error en {ym}: {e}")
            _time.sleep(3)
            continue
        except requests.exceptions.Timeout as e:
            if verbose:
                print(f"  ! Timeout en {ym}: {e}")
            _time.sleep(2)
            continue
        except requests.exceptions.RequestException as e:
            if verbose:
                print(f"  ! Network error en {ym}: {e}")
            _time.sleep(1)
            continue
        finally:
            # Rate limiting: respetar el servidor — minimo 1.5s entre requests
            _time.sleep(1.5)

    if df_history:
        result = pd.concat(df_history, ignore_index=True)
        # Eliminar columna auxiliar de periodo si no se quiere exponer
        if '_periodo' in result.columns:
            result = result.drop(columns=['_periodo'])
        return result
    return None


def _get_remote_params_for_station(cod, verbose=True):
    """
    Busca la estacion en la lista REMOTA del portal SENAMHI (array JS PruebaTest)
    y devuelve un dict con los campos reales: ico, estado, cod_old.

    La lista remota tiene los valores exactos que export.php espera:
      - ico:    'M' o 'H'
      - estado: 'DIFERIDO', 'REAL' o 'AUTOMATICA'

    Devuelve None si no se puede obtener la lista remota o no se encuentra la estacion.
    """
    if not cod or str(cod).strip() in ('', 'nan', 'None'):
        return None
    cod_str = str(cod).strip().upper()
    try:
        df_remote = get_stations_senamhi()
    except Exception as e:
        if verbose:
            print(f"  ! No se pudo obtener lista remota: {e}")
        return None

    # Buscar por codigo exacto (campo 'cod' en la lista remota)
    mask = df_remote['cod'].astype(str).str.strip().str.upper() == cod_str
    if not mask.any():
        # Intentar por cod_old
        if 'cod_old' in df_remote.columns:
            mask = df_remote['cod_old'].astype(str).str.strip().str.upper() == cod_str
    if not mask.any():
        if verbose:
            print(f"  ! cod={cod} no encontrado en lista remota.")
        return None

    row_remote = df_remote[mask].iloc[0]
    ico_r = str(row_remote.get('ico') or '').strip().upper()
    estado_r = str(row_remote.get('estado') or '').strip().upper()
    cod_old_r = row_remote.get('cod_old')

    if ico_r not in ('M', 'H'):
        ico_r = 'M'
    ESTADOS_PORTAL = {'DIFERIDO', 'REAL', 'AUTOMATICA'}
    if estado_r not in ESTADOS_PORTAL:
        estado_r = 'REAL'

    return {
        'ico': ico_r,
        'estado': estado_r,
        'cod_old': None if (not cod_old_r or str(cod_old_r).strip() in ('', 'nan', 'None')) else str(cod_old_r).strip(),
    }


def download_station_data_robust(row, from_date, to_date, verbose=True):
    """
    Version robusta de download_station_data que:
    1. Primero intenta obtener ico y estado reales desde la lista REMOTA del portal
       (igual que el script run_orcopampa_full.py que funciono).
    2. Si la lista remota no esta disponible o no devuelve datos, prueba multiples
       combinaciones heuristicas de (cod, ico, estado_portal).

    Esta es la funcion que deben usar save_station_by_index y el dashboard,
    especialmente cuando los datos provienen de la Maestra local (donde ico y
    estado_portal no coinciden con el formato de export.php).

    Args:
        row: dict o Series con al menos 'cod', y opcionalmente 'ico', 'estado',
             'cod_old', 'CONVENCIONAL', 'AUTOMATICA'
        from_date: Fecha inicio 'YYYY-MM-DD'
        to_date: Fecha fin 'YYYY-MM-DD'
        verbose: Mostrar progreso

    Returns: (DataFrame, cod_usado, ico_usado, estado_usado) o (None, None, None, None)
    """
    import math as _math

    def _is_empty(v):
        if v is None:
            return True
        if isinstance(v, float) and _math.isnan(v):
            return True
        return str(v).strip() in ('', 'nan', 'None')

    cod_raw = row.get('cod')
    if _is_empty(cod_raw):
        if verbose:
            print("  ! Estacion sin codigo — no se puede descargar.")
        return None, None, None, None

    cod = str(cod_raw).strip()

    # --- PASO 1: intentar con parametros reales de la lista REMOTA ---
    # Este es exactamente el metodo que uso run_orcopampa_full.py y que funciono.
    # Si el campo 'ico' ya viene con un valor valido del portal (porque el caller
    # cargo la lista remota previamente), usarlo directamente sin hacer otra peticion.
    ico_precargado = str(row.get('ico') or '').strip().upper()
    estado_precargado = str(row.get('estado') or '').strip().upper()
    ESTADOS_PORTAL = {'DIFERIDO', 'REAL', 'AUTOMATICA'}

    if ico_precargado in ('M', 'H') and estado_precargado in ESTADOS_PORTAL:
        # Los parametros del portal ya estan disponibles en la fila (lista remota previa)
        if verbose:
            print(f"  > Usando parametros precargados: ico={ico_precargado} estado={estado_precargado}")
        cod_old_val = None if _is_empty(row.get('cod_old')) else str(row.get('cod_old')).strip()
        df_direct = download_station_data(
            cod=cod,
            ico=ico_precargado,
            estado=estado_precargado,
            cod_old=cod_old_val,
            from_date=from_date,
            to_date=to_date,
            verbose=verbose,
        )
        if df_direct is not None and len(df_direct) > 0:
            return df_direct, cod, ico_precargado, estado_precargado
        if verbose:
            print(f"  ! Parametros precargados no dieron datos — intentando con lista remota...")

    # --- PASO 2: buscar parametros reales en la lista REMOTA del portal ---
    if verbose:
        print(f"  > Consultando lista remota del portal para cod={cod}...")
    remote_params = _get_remote_params_for_station(cod, verbose=verbose)
    if remote_params is not None:
        r_ico = remote_params['ico']
        r_estado = remote_params['estado']
        r_cod_old = remote_params['cod_old']
        if verbose:
            print(f"  > Lista remota: ico={r_ico} estado={r_estado} cod_old={r_cod_old}")
        df_remote = download_station_data(
            cod=cod,
            ico=r_ico,
            estado=r_estado,
            cod_old=r_cod_old,
            from_date=from_date,
            to_date=to_date,
            verbose=verbose,
        )
        if df_remote is not None and len(df_remote) > 0:
            return df_remote, cod, r_ico, r_estado
        if verbose:
            print(f"  ! Lista remota no dio datos — probando combinaciones heuristicas...")

    # --- PASO 3: bruteforce heuristico con candidatos de _resolve_portal_params ---
    candidates = _resolve_portal_params(row)

    if not candidates:
        if verbose:
            print("  ! No se pudieron determinar parametros de descarga para esta estacion.")
        return None, None, None, None

    if verbose:
        print(f"  > Probando {len(candidates)} combinaciones heuristicas...")

    # Probar una muestra rapida (1 mes) con cada candidato antes de descargar completo
    # Esto evita gastar muchos segundos en combinaciones incorrectas
    import time as _time

    test_date = pd.date_range(from_date, to_date, freq='MS')
    if len(test_date) == 0:
        return None, None, None, None

    # Usar el primer mes disponible como prueba rapida
    test_ym = test_date[0].strftime('%Y%m')

    session = _build_senamhi_session()
    base_url = 'https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/export.php'

    working_candidate = None

    for cand in candidates:
        c_cod = cand['cod']
        c_ico = cand['ico']
        c_est = cand['estado_portal']
        c_old = cand['cod_old']

        params = {
            'estaciones': c_cod,
            'CBOFiltro': test_ym,
            't_e': c_ico,
            'estado': c_est,
        }
        if c_old:
            params['cod_old'] = c_old

        param_str = '&'.join(f"{k}={v}" for k, v in params.items())
        link = f"{base_url}?{param_str}"

        if verbose:
            print(f"  ? Probando cod={c_cod} t_e={c_ico} estado={c_est} ({test_ym})...")

        try:
            resp = session.get(link, timeout=30)
            _time.sleep(1.0)
        except Exception as e:
            if verbose:
                print(f"    Error de red: {e}")
            _time.sleep(1.5)
            continue

        if resp.status_code != 200:
            if verbose:
                print(f"    HTTP {resp.status_code} — saltando")
            continue

        df_test = _parse_export_html(resp.text)
        if df_test is not None and len(df_test) > 0:
            if verbose:
                print(f"  + Combinacion exitosa: cod={c_cod} t_e={c_ico} estado={c_est}")
            working_candidate = cand
            break
        else:
            if verbose:
                print(f"    Sin datos para este candidato.")

    if working_candidate is None:
        if verbose:
            print("  ! Ninguna combinacion devolvio datos en el mes de prueba.")
        return None, None, None, None

    # Descargar el rango completo con la combinacion que funciono
    if verbose:
        print(f"  Descargando rango completo con cod={working_candidate['cod']} "
              f"t_e={working_candidate['ico']} estado={working_candidate['estado_portal']}...")

    df_full = download_station_data(
        cod=working_candidate['cod'],
        ico=working_candidate['ico'],
        estado=working_candidate['estado_portal'],
        cod_old=working_candidate['cod_old'],
        from_date=from_date,
        to_date=to_date,
        verbose=verbose,
    )

    return (
        df_full,
        working_candidate['cod'],
        working_candidate['ico'],
        working_candidate['estado_portal'],
    )


def get_station_data(station_name, from_date='2024-01-01', to_date='2024-12-31', df_stations=None):
    """
    Obtiene datos de una estación específica por nombre

    Args:
        station_name: Nombre de la estación
        from_date: Fecha inicio
        to_date: Fecha fin
        df_stations: DataFrame de estaciones (si no se proporciona, se descarga)

    Returns: DataFrame con datos
    """
    if df_stations is None:
        df_stations = get_stations_senamhi()

    # Buscar estación
    station = search_stations(df_stations, name=station_name)

    if len(station) == 0:
        print(f"✗ No se encontró la estación '{station_name}'")
        return None

    if len(station) > 1:
        print(f"⚠ Se encontraron {len(station)} estaciones con ese nombre:")
        print(station[['estacion', 'categoria', 'estado', 'cod']])
        print("\nUsando la primera...")
        station = station.iloc[0:1]

    station = station.iloc[0]

    print(f"📌 Descargando datos de: {station['estacion']}")
    print(f"   Categoría: {station['categoria']}")
    print(f"   Estado: {station['estado']}")
    print(f"   Coordenadas: ({station['lat']}, {station['lon']})")

    # Descargar datos
    df_data = download_station_data(
        station['cod'],
        station['ico'],
        station['estado'],
        station['cod_old'],
        from_date,
        to_date
    )

    if df_data is not None:
        print(f"✓ {len(df_data)} registros descargados")
        print(f"\nColumnas disponibles:")
        for i, col in enumerate(df_data.columns, 1):
            print(f"   {i}. {col}")
        return df_data
    else:
        print("✗ No se encontraron datos")
        return None


def filter_stations_by_shapefile(df_stations, shapefile_path):
    """
    Filtra estaciones que están dentro de un shapefile

    Args:
        df_stations: DataFrame de estaciones
        shapefile_path: Ruta al archivo .shp

    Returns: DataFrame con estaciones dentro del área
    """
    print(f"📂 Cargando shapefile: {shapefile_path}")
    gdf_area = gpd.read_file(shapefile_path)

    # Crear GeoDataFrame de estaciones
    geometry = [Point(xy) for xy in zip(df_stations['lon'], df_stations['lat'])]
    gdf_stations = gpd.GeoDataFrame(df_stations, geometry=geometry, crs='EPSG:4326')

    # Asegurar mismo CRS
    if gdf_area.crs != gdf_stations.crs:
        gdf_stations = gdf_stations.to_crs(gdf_area.crs)

    # Filtrar estaciones dentro del área
    stations_inside = gpd.sjoin(gdf_stations, gdf_area, how='inner', predicate='within')
    stations_inside = stations_inside[~stations_inside.index.duplicated(keep='first')]

    # Eliminar columna geometry para facilitar el trabajo
    if 'geometry' in stations_inside.columns:
        stations_inside = stations_inside.drop('geometry', axis=1)

    print(f"✓ {len(stations_inside)} estaciones dentro del área")
    return stations_inside
def load_local_stations(path=None):
    """Lee un archivo Excel local con la maestra de estaciones.

    Busca por defecto en `DATA/Maestra_de_estaciones_Senamhi.xlsx`.
    Devuelve un DataFrame con columnas compatibles con las funciones.
    """
    if path is None:
        path = Path(__file__).resolve().parents[0] / 'DATA' / 'Maestra_de_estaciones_Senamhi.xlsx'
    else:
        path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {path}")

    df = pd.read_excel(path)

    # Normalizar nombres de columnas comunes (incluye variantes en español)
    mapping = {}
    cols = {c.lower(): c for c in df.columns}

    # Nombres de estación
    if 'estacion' in cols:
        mapping[cols['estacion']] = 'estacion'
    elif 'nombre_estacion' in cols:
        mapping[cols['nombre_estacion']] = 'estacion'
    elif 'nombre' in cols:
        mapping[cols['nombre']] = 'estacion'

    # Código
    if 'cod' in cols:
        mapping[cols['cod']] = 'cod'
    elif 'codigo' in cols:
        mapping[cols['codigo']] = 'cod'

    # Tipo / icono
    if 'ico' in cols:
        mapping[cols['ico']] = 'ico'

    # Código antiguo
    if 'cod_old' in cols:
        mapping[cols['cod_old']] = 'cod_old'

    # Coordenadas (variantes en español)
    if 'lat' in cols:
        mapping[cols['lat']] = 'lat'
    elif 'latitud' in cols:
        mapping[cols['latitud']] = 'lat'

    if 'lon' in cols:
        mapping[cols['lon']] = 'lon'
    elif 'longitud' in cols:
        mapping[cols['longitud']] = 'lon'

    # Otros
    if 'categoria' in cols:
        mapping[cols['categoria']] = 'categoria'
    if 'estado' in cols:
        mapping[cols['estado']] = 'estado'

    if mapping:
        df = df.rename(columns=mapping)

    # Asegurar columnas mínimas
    for c in ['estacion', 'cod', 'ico', 'cod_old', 'lat', 'lon', 'categoria', 'estado']:
        if c not in df.columns:
            df[c] = None

    # Convertir coordenadas
    df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
    df['lon'] = pd.to_numeric(df['lon'], errors='coerce')

    # Eliminar filas sin coordenadas
    df = df.dropna(subset=['lat', 'lon'])

    return df


def process_all_stations(df_stations, from_date='2024-01-01', to_date='2024-12-31', output_dir=None, limit=None, verbose=True):
    """Itera sobre el DataFrame de estaciones y descarga los datos para cada una.

    Guarda un archivo por estación en `output_dir` (xlsx). Devuelve lista de rutas guardadas.
    """
    if output_dir is None:
        output_dir = Path(__file__).resolve().parents[0] / 'DATA' / 'outputs'
    else:
        output_dir = Path(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    saved_files = []
    total = len(df_stations)
    if limit is not None:
        df_iter = df_stations.iloc[:limit]
    else:
        df_iter = df_stations

    for idx, row in df_iter.iterrows():
        name = re.sub(r'[^A-Za-z0-9_-]', '_', str(row.get('estacion') or f'station_{idx}'))
        cod = row.get('cod') or ''
        filename = f"{idx:04d}_{name}_{cod}.xlsx"
        outpath = output_dir / filename

        if verbose:
            print(f"\n➡ Procesando [{idx+1}/{total}]: {row.get('estacion')} -> {outpath.name}")

        try:
            df = download_station_data(row.get('cod'), row.get('ico'), row.get('estado'), row.get('cod_old'), from_date, to_date, verbose=False)
            if df is not None and len(df) > 0:
                # Guardar
                    try:
                        try:
                            df_to_save = _normalize_downloaded_df(df)
                        except Exception:
                            df_to_save = df
                        df_to_save.to_excel(outpath, index=False)
                        saved_files.append(outpath)
                    if verbose:
                        print(f"   ✓ Guardado: {outpath}")
                except Exception as e:
                    if verbose:
                        print(f"   ⚠ Error guardando {outpath}: {e}")
            else:
                if verbose:
                    print("   ✗ No se encontraron datos para esta estación")
        except Exception as e:
            if verbose:
                print(f"   ⚠ Error procesando estación: {e}")

    return saved_files


# ----------------------------------------
# INTERFAZ PARA DASHBOARD / USO EXTERNO
# ----------------------------------------

# Directorio de datos por defecto (puede ser reconfigurado por el dashboard)
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[0] / 'DATA'

def get_stations(use_local=True, local_path=None):
    """Devuelve el DataFrame de estaciones.

    - Si `use_local` es True intenta leer `local_path` o el archivo por defecto
      `DATA/Maestra_de_estaciones_Senamhi.xlsx`.
    - Si falla (o `use_local` es False) se descargan las estaciones desde SENAMHI.
    """
    if use_local:
        lp = Path(local_path) if local_path else DEFAULT_DATA_DIR / 'Maestra_de_estaciones_Senamhi.xlsx'
        try:
            return load_local_stations(lp)
        except Exception:
            return get_stations_senamhi()
    else:
        return get_stations_senamhi()


def save_station_by_index(df_stations, index, from_date, to_date, output_dir=None):
    """Descarga y guarda los datos de la estacion indicada por indice del DataFrame.

    Usa download_station_data_robust() para probar multiples combinaciones de
    parametros (ico, estado) hasta encontrar la que devuelve datos reales. Esto
    es necesario porque la Maestra local no tiene los campos 'ico' y 'estado' en
    el formato que espera export.php del portal SENAMHI.

    Devuelve la ruta del archivo guardado o None si no hay datos.
    """
    output_dir = Path(output_dir) if output_dir else DEFAULT_DATA_DIR / 'outputs'
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        row = df_stations.iloc[index]
    except Exception:
        raise IndexError('Indice de estacion fuera de rango')

    name = re.sub(r'[^A-Za-z0-9_-]', '_', str(row.get('estacion') or f'station_{index}'))
    cod = row.get('cod') or ''
    filename = f"{index:04d}_{name}_{cod}.xlsx"
    outpath = output_dir / filename

    # Usar la version robusta que prueba multiples combinaciones de parametros
    df, used_cod, used_ico, used_estado = download_station_data_robust(
        row, from_date, to_date, verbose=True
    )

    if df is not None and len(df) > 0:
        try:
            df_clean = _normalize_downloaded_df(df)
        except Exception:
            df_clean = df
        df_clean.to_excel(outpath, index=False)
        return outpath

    # Si no hay datos, crear un archivo marcador .xlsx con metadatos para dejar rastro
    try:
        import pandas as _pd
        meta = _pd.DataFrame([{
            'estacion': row.get('estacion'),
            'cod': row.get('cod'),
            'from_date': from_date,
            'to_date': to_date,
            'status': 'no_data_or_no_tables'
        }])
        meta.to_excel(outpath, index=False)
        return outpath
    except Exception:
        return None


def save_station_by_name(df_stations, station_name, from_date, to_date, output_dir=None):
    """Busca la estación por nombre y guarda sus datos. Devuelve la ruta o None."""
    found = search_stations(df_stations, name=station_name)
    if len(found) == 0:
        return None
    row = found.iloc[0]
    idx = df_stations.index.get_loc(row.name) if row.name in df_stations.index else None
    if idx is None:
        # fallback: use iloc on the filtered frame
        return save_station_by_index(found.reset_index(drop=True), 0, from_date, to_date, output_dir)
    return save_station_by_index(df_stations, idx, from_date, to_date, output_dir)


# NOTA: No se ejecuta nada al importar este módulo. El dashboard debe importar
# las funciones `get_stations`, `get_station_data`, `process_all_stations`,
# `save_station_by_index` o `save_station_by_name` según su necesidad.


def save_each_station_verbose(from_date='2015-06-11', to_date=None, use_local=True, output_dir=None, limit=None, verbose=True):
    """
    Ejecuta la misma lógica de `save_santa_full.py` para cada estación disponible.

    - Carga la lista de estaciones (local o remota según `use_local`).
    - Para cada estación llama a `save_station_by_name` y luego verifica los
      datos en memoria con `get_station_data`.

    Devuelve la lista de rutas guardadas (pueden ser None cuando no hay datos).
    """
    if to_date is None:
        to_date = datetime.now().strftime('%Y-%m-%d')

    df = get_stations(use_local=use_local)
    if limit is not None:
        df_iter = df.iloc[:limit]
    else:
        df_iter = df

    saved_results = []

    # Usar una copia con índices secuenciales para que `save_station_by_name`
    # funcione consistentemente cuando necesite reindexar.
    df_for_save = df.reset_index(drop=True)

    total = len(df_iter)
    for i, row in df_iter.reset_index(drop=True).iterrows():
        station_name = row.get('estacion')
        if verbose:
            print('\n===========================================')
            print(f"Procesando estación {i+1}/{total}: {station_name}")

        out = save_station_by_name(df_for_save, station_name, from_date, to_date, output_dir)
        if verbose:
            print('Resultado guardado en:', out)

        if out:
            if verbose:
                print('Comprobando contenido en memoria...')
            data = get_station_data(station_name, from_date, to_date, df_stations=df_for_save)
            if data is None:
                if verbose:
                    print('No se descargaron registros en memoria.')
            else:
                if verbose:
                    print('Registros descargados en memoria:', len(data))

        saved_results.append(out)

    return saved_results
