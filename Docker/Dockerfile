# ============================================================
# SENAMHI Dashboard - Dockerfile
# Base: python:3.11-slim (Debian Bookworm)
# ============================================================

FROM python:3.11-slim

# --- Variables de build ---
ARG DEBIAN_FRONTEND=noninteractive

# -----------------------------------------------------------
# 1. Dependencias de sistema
#    - build-essential + gcc/g++  : compilar wheels nativos
#    - gdal-bin + libgdal-dev     : GDAL (gdal-config --version
#                                   lo usa fiona al compilarse)
#    - libgeos-dev                : shapely
#    - libproj-dev + proj-bin     : pyproj
#    - proj-data                  : archivos de proyeccion PROJ
#    - libxml2-dev + libxslt1-dev : lxml con soporte XSLT
#    - curl                       : healthcheck en docker-compose
# -----------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    g++ \
    gdal-bin \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    proj-bin \
    proj-data \
    libxml2-dev \
    libxslt1-dev \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------
# 2. Variables de entorno
#    Se declaran antes de pip install para que pyproj/fiona
#    puedan encontrar PROJ_LIB durante la compilacion.
# -----------------------------------------------------------
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PROJ_LIB=/usr/share/proj

# -----------------------------------------------------------
# 3. Directorio de trabajo
# -----------------------------------------------------------
WORKDIR /app

# -----------------------------------------------------------
# 4. Instalar dependencias Python
#    - Se copia solo requirements.txt primero para aprovechar
#      cache de capas: si el archivo no cambia, esta capa
#      no se reconstruye aunque cambien los .py.
#    - GDAL se instala por separado con la version exacta
#      del sistema para que el binding Python coincida.
# -----------------------------------------------------------
COPY requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel

RUN GDAL_VERSION=$(gdal-config --version) && \
    pip install --no-cache-dir \
        "GDAL==${GDAL_VERSION}" \
        -r /app/requirements.txt

# -----------------------------------------------------------
# 5. Copiar codigo fuente (explicito, sin COPY . /app)
# -----------------------------------------------------------
COPY app.py                    /app/app.py
COPY server.py                 /app/server.py
COPY dashboard_hidrometeo.html /app/dashboard_hidrometeo.html

# -----------------------------------------------------------
# 6. Copiar datos estaticos (shapefiles + Excel maestra)
#    DATA/outputs/ esta excluido en .dockerignore, por lo
#    que no se bake en la imagen; se monta como volumen.
# -----------------------------------------------------------
COPY DATA/ /app/DATA/

# -----------------------------------------------------------
# 7. Crear directorio de outputs dentro del contenedor.
#    El volumen nombrado se montara sobre este directorio.
# -----------------------------------------------------------
RUN mkdir -p /app/DATA/outputs

# -----------------------------------------------------------
# 8. Puerto expuesto
# -----------------------------------------------------------
EXPOSE 5000

# -----------------------------------------------------------
# 9. Comando de arranque con gunicorn
#    --workers 2     : adecuado para 1-2 vCPU en contenedor
#    --timeout 120   : rutas con scraping al vuelo pueden
#                      tardar; evita que gunicorn mate workers
#    --access-logfile - : logs a stdout (visible en docker logs)
# -----------------------------------------------------------
CMD ["gunicorn", \
     "--workers", "2", \
     "--bind", "0.0.0.0:5000", \
     "--timeout", "120", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "server:app"]
