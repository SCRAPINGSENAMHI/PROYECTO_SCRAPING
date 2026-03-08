"""
Script sencillo para verificar las librerías listadas en `requirements.txt`.
Ejecutar: python scripts/check_requirements.py
"""
from pkg_resources import get_distribution, DistributionNotFound
from pathlib import Path

REQ_FILE = Path(__file__).resolve().parents[0].parent / 'requirements.txt'

def parse_requirements(path):
    reqs = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.split('#', 1)[0].strip()
            if not line:
                continue
            if '==' in line:
                name, ver = line.split('==', 1)
                reqs.append((name.strip(), ver.strip()))
            else:
                reqs.append((line.strip(), None))
    return reqs

def main():
    reqs = parse_requirements(REQ_FILE)
    for name, required_version in reqs:
        try:
            installed = get_distribution(name).version
            if required_version and installed != required_version:
                print(f"{name}: {installed} (instalado) != requerido {required_version}")
            else:
                print(f"{name}: {installed} (instalado correctamente)")
        except DistributionNotFound:
            print(f"{name}: No se encontró la librería")

if __name__ == '__main__':
    main()
