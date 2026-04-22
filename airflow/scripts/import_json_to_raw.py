from __future__ import annotations

import os
import shutil
from pathlib import Path

SOURCE_FILE = Path(os.getenv('SOURCE_JSON_FILE', '/opt/airflow/sample_data/orders_2026_04_21.json'))
RAW_DIR = Path(os.getenv('RAW_LOCAL_DIR', '/opt/airflow/shared/raw/orders'))

RAW_DIR.mkdir(parents=True, exist_ok=True)
if not SOURCE_FILE.exists():
    raise FileNotFoundError(f'Source JSON file not found: {SOURCE_FILE}')

target = RAW_DIR / SOURCE_FILE.name
shutil.copy2(SOURCE_FILE, target)
print(f'Imported {SOURCE_FILE} -> {target}')
