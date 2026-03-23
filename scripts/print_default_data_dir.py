from pathlib import Path
import importlib, sys
sys.path.append(str(Path(__file__).resolve().parents[1]))
mod = importlib.import_module('app.app')
print('DEFAULT_DATA_DIR =', mod.DEFAULT_DATA_DIR)
print('exists?', mod.DEFAULT_DATA_DIR.exists())
print('listing:', list(mod.DEFAULT_DATA_DIR.iterdir())[:20])
