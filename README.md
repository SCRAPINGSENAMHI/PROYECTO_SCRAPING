# Web_Scraping_SENAMHI — Instrucciones rápidas

Resumen
- Proyecto para extraer y servir estaciones SENAMHI y alimentar el `dashboard_hidrometeo.html`.
- El servidor local (Flask) sirve el HTML y ofrece endpoints API para listar estaciones y guardar datos.

Requisitos
- Python 3.10+ (Windows)
- Conexión a internet para descargar dependencias y tiles de mapas

Instalación (Windows)
1. Crear y activar entorno virtual (cmd):

```bat
python -m venv .venv
.venv\Scripts\activate
```

En PowerShell:

```powershell
python -m venv .venv
. .venv\Scripts\Activate.ps1
```

2. Instalar dependencias:

```powershell
pip install --upgrade pip
pip install -r requirements.txt
```

Ejecutar servidor (desarrollo)

```powershell
python server.py
```

Abrir en el navegador: http://127.0.0.1:5000

Archivos clave
- `app.py`: lógica de scraping y funciones públicas (`get_stations`, `process_all_stations`, `save_station_by_name`, ...).
- `server.py`: servidor Flask que sirve el dashboard y expone API:
  - `GET /api/stations` → lista de estaciones (incluye `lat`, `lon`, `cod`).
  - `POST /api/save_by_name` → guarda datos de una estación (payload JSON: `station_name`, `from_date`, `to_date`).
  - `POST /api/process_all` → procesa varias estaciones (payload JSON: `from_date`, `to_date`, `limit`).
- `dashboard_hidrometeo.html`: frontend que consume `GET /api/stations` y muestra el mapa (Leaflet, ESRI WorldImagery).
- `DATA/Maestra_de_estaciones_Senamhi.xlsx`: archivo maestro local (si existe, se usa). Si no existe, el sistema descarga desde SENAMHI.
- `DATA/outputs/`: carpeta donde se guardan los archivos xlsx por estación.

Uso de la API (ejemplos)

- Listar estaciones (GET):

```bash
curl http://127.0.0.1:5000/api/stations
```

- Guardar estación por nombre (POST):

```bash
curl -X POST http://127.0.0.1:5000/api/save_by_name -H "Content-Type: application/json" -d '{"station_name":"SANTA ANITA","from_date":"2024-01-01","to_date":"2024-12-31"}'
```

- Procesar varias estaciones (POST, con `limit` opcional):

```bash
curl -X POST http://127.0.0.1:5000/api/process_all -H "Content-Type: application/json" -d '{"from_date":"2024-01-01","to_date":"2024-12-31","limit":10}'
```

Notas y recomendaciones
- Asegúrate de que `DATA/Maestra_de_estaciones_Senamhi.xlsx` contenga las columnas `estacion`, `lat`, `lon`, `cod` o nombres equivalentes (el loader intenta normalizar nombres de columna).
- El dashboard usa ESRI WorldImagery tiles; si prefieres Mapbox/Bing necesitarás una clave y cambiar la URL en `dashboard_hidrometeo.html`.
- Para producción considera servir Flask con un WSGI (gunicorn/uvicorn) y proteger los endpoints.

Soporte
- Si quieres, puedo: arrancar el servidor aquí y pegar la salida; ajustar el `limit` por defecto; o añadir un endpoint que devuelva un archivo combinado con todos los datos.
