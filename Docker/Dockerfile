FROM python:3.11-slim

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc g++ \
    gdal-bin libgdal-dev \
    libgeos-dev libproj-dev proj-bin proj-data \
    libxml2-dev libxslt1-dev \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PROJ_LIB=/usr/share/proj

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip setuptools wheel && \
    GDAL_VERSION=$(gdal-config --version) && \
    pip install --no-cache-dir "GDAL==${GDAL_VERSION}" -r /app/requirements.txt

# Codigo fuente (estructura actual: todo en app/)
COPY app/app.py                    /app/app.py
COPY app/server.py                 /app/server.py
COPY app/dashboard_historico.html  /app/dashboard_historico.html
COPY app/dashboard_hidrometeo.html /app/dashboard_hidrometeo.html
COPY app/static/                   /app/static/
COPY app/scripts/                  /app/scripts/

# Datos estaticos (shapefiles, Excel maestra)
# DATA/outputs excluido via .dockerignore - se monta como volumen
COPY DATA/ /app/DATA/
RUN mkdir -p /app/DATA/outputs

EXPOSE 5000

CMD ["gunicorn", \
     "--workers", "2", \
     "--bind", "0.0.0.0:5000", \
     "--timeout", "120", \
     "--access-logfile", "-", \
     "--error-logfile", "-", \
     "server:app"]
