import subprocess
from pathlib import Path
from datetime import datetime

def list_python_procs():
    try:
        out = subprocess.check_output(['tasklist', '/FI', 'IMAGENAME eq python.exe'], text=True, stderr=subprocess.DEVNULL)
        print('--- Python processes (tasklist) ---')
        print(out)
    except Exception as e:
        print('Could not list processes:', e)

def list_santa_files():
    p = Path('DATA') / 'outputs'
    print('--- DATA/outputs exists:', p.exists(), '---')
    if not p.exists():
        return
    santa = sorted(p.glob('*SANTA*'))
    print('Files matching *SANTA*:', len(santa))
    for f in santa:
        st = f.stat()
        print(f.name, '-', st.st_size, 'bytes -', datetime.fromtimestamp(st.st_mtime))
    print('--- Recent .xlsx (10) ---')
    xlsx = sorted(p.glob('*.xlsx'), key=lambda x: x.stat().st_mtime, reverse=True)[:10]
    for f in xlsx:
        st = f.stat()
        print(f.name, '-', st.st_size, 'bytes -', datetime.fromtimestamp(st.st_mtime))

if __name__ == '__main__':
    list_python_procs()
    list_santa_files()
