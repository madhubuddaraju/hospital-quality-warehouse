"""Load the committed real-data sample (three states) into data/raw, so the warehouse builds without downloading.

python scripts/use_fixtures.py
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures"
CUR = ROOT / "data" / "raw" / "current"
HIST = ROOT / "data" / "raw" / "history"

CUR.mkdir(parents=True, exist_ok=True)
HIST.mkdir(parents=True, exist_ok=True)
for f in sorted(FIX.glob("*.csv")):
    df = pd.read_csv(f, dtype=str, keep_default_na=False)
    if f.stem.startswith("history_"):
        df.to_parquet(HIST / f"hospital_general_{f.stem.removeprefix('history_')}.parquet", index=False)
    else:
        df.to_parquet(CUR / f"{f.stem}.parquet", index=False)
    print(f"{f.name}: {len(df)} rows")
