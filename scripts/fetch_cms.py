"""Download Medicare hospital quality data, Census ZIP centroids and past quarterly releases.

python scripts/fetch_cms.py                 # current release + ZIP centroids -> data/raw/
python scripts/fetch_cms.py --history       # also past releases of Hospital General Information
python scripts/fetch_cms.py --profile       # also write reports/source_profile.json and test fixtures

Files land as parquet with snake_case column names, so dbt models see stable names.
"""

import argparse
import hashlib
import io
import json
import re
import sys
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
CURRENT = RAW / "current"
HISTORY = RAW / "history"
FIXTURES = ROOT / "tests" / "fixtures"
REPORTS = ROOT / "reports"

METASTORE = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items/{}"
ARCHIVE = "https://data.cms.gov/provider-data/sites/default/files/archive/Hospitals/{y}/hospitals_{m:02d}_{y}.zip"
ZCTA = "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/{y}_Gazetteer/{y}_Gaz_zcta_national.zip"

# name -> CMS Provider Data Catalog dataset id
DATASETS = {
    "hospital_general": "xubh-q36u",        # Hospital General Information (one row per hospital, overall star rating)
    "unplanned_visits": "632h-zaca",        # Unplanned Hospital Visits (readmissions, return days)
    "complications_deaths": "ynj2-r877",    # Complications and Deaths (30-day mortality, PSI-90, hip/knee complications)
    "timely_care": "yv7e-xc69",             # Timely and Effective Care (ER times, sepsis care)
    "patient_survey": "dgck-syfz",          # HCAHPS patient survey
    "infections": "77hc-ibv8",              # Healthcare-Associated Infections
}

S = requests.Session()
S.headers["User-Agent"] = "hospital-quality-warehouse (github.com/madhubuddaraju/hospital-quality-warehouse)"


def get(url: str, tries: int = 4, **kw) -> requests.Response:
    for i in range(tries):
        try:
            r = S.get(url, timeout=120, **kw)
            if r.status_code == 404:
                return r
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if i == tries - 1:
                raise
            print(f"  retry {i + 1} after {e}", flush=True)
            time.sleep(5 * (i + 1))
    raise RuntimeError("unreachable")


def snake(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()
    return re.sub(r"_+", "_", s)


def read_csv(blob: bytes) -> pd.DataFrame:
    df = pd.read_csv(io.BytesIO(blob), dtype=str, keep_default_na=False, encoding="utf-8-sig")
    df.columns = [snake(c) for c in df.columns]
    return df


def fetch_current() -> dict:
    CURRENT.mkdir(parents=True, exist_ok=True)
    manifest = {}
    for name, ds in DATASETS.items():
        meta = get(METASTORE.format(ds)).json()
        url = meta["distribution"][0]["downloadURL"]
        blob = get(url).content
        df = read_csv(blob)
        df["_cms_modified"] = meta.get("modified")
        df.to_parquet(CURRENT / f"{name}.parquet", index=False)
        manifest[name] = {"dataset_id": ds, "title": meta.get("title"), "modified": meta.get("modified"), "url": url,
                          "rows": len(df), "columns": list(df.columns), "sha256": hashlib.sha256(blob).hexdigest()}
        print(f"{name}: {len(df):,} rows, modified {meta.get('modified')}", flush=True)
    return manifest


def fetch_zcta() -> dict:
    for y in range(datetime.now(UTC).year, 2019, -1):
        r = get(ZCTA.format(y=y))
        if r.status_code == 200:
            with zipfile.ZipFile(io.BytesIO(r.content)) as z:
                txt = z.read(z.namelist()[0]).decode("utf-8")
            df = pd.read_csv(io.StringIO(txt), sep="\t", dtype=str)
            df.columns = [c.strip().lower().lstrip("\ufeff") for c in df.columns]

            def col(part: str) -> str:
                hit = [c for c in df.columns if part in c]
                if not hit:
                    raise SystemExit(f"Census file {y} has no column like {part!r}: {list(df.columns)}")
                return hit[0]
            out = pd.DataFrame({"zip": df[col("geoid")].str.strip().str[-5:].str.zfill(5),
                                "lat": df[col("intptlat")].astype(float), "lon": df[col("intptlong")].astype(float)})
            out.to_parquet(CURRENT / "zip_centroids.parquet", index=False)
            print(f"zip centroids: {len(out):,} ZIP codes from the {y} Census gazetteer", flush=True)
            return {"year": y, "rows": len(out), "url": ZCTA.format(y=y)}
    raise SystemExit("no Census ZCTA gazetteer found")


def general_member(names: list[str]) -> str | None:
    hits = [n for n in names if re.search(r"hospital_general_information", n, re.I) and n.lower().endswith(".csv")]
    return min(hits, key=len) if hits else None


def fetch_history(start_year: int = 2021) -> list[dict]:
    """Past releases of Hospital General Information, extracted from CMS's monthly archive zips.

    Reads only the one CSV needed from each zip using HTTP range requests (the zips are hundreds of MB).
    """
    from remotezip import RemoteZip

    HISTORY.mkdir(parents=True, exist_ok=True)
    found = []
    now = datetime.now(UTC)
    for y in range(start_year, now.year + 1):
        for m in range(1, 13):
            if (y, m) > (now.year, now.month):
                break
            url = ARCHIVE.format(y=y, m=m)
            out = HISTORY / f"hospital_general_{y}_{m:02d}.parquet"
            if out.exists():
                found.append({"release": f"{y}-{m:02d}", "url": url, "cached": True})
                continue
            if S.head(url, timeout=60, allow_redirects=True).status_code != 200:
                continue
            try:
                with RemoteZip(url, session=S) as z:
                    member = general_member(z.namelist())
                    if member is None:
                        print(f"  {y}-{m:02d}: no general information file in archive", flush=True)
                        continue
                    blob = z.read(member)
            except Exception as e:  # server without range support: fall back to a full download
                print(f"  {y}-{m:02d}: range read failed ({e}); downloading whole archive", flush=True)
                with zipfile.ZipFile(io.BytesIO(get(url).content)) as z:
                    member = general_member(z.namelist())
                    if member is None:
                        continue
                    blob = z.read(member)
            df = read_csv(blob)
            df["_release"] = f"{y}-{m:02d}-01"
            df.to_parquet(out, index=False)
            found.append({"release": f"{y}-{m:02d}", "url": url, "member": member, "rows": len(df)})
            print(f"history {y}-{m:02d}: {len(df):,} hospitals ({member})", flush=True)
    return found


def profile(manifest: dict) -> None:
    """Columns, measure ids and a small fixture of real rows (three states) for tests and local development."""
    REPORTS.mkdir(exist_ok=True)
    FIXTURES.mkdir(parents=True, exist_ok=True)
    prof = {}
    gen = pd.read_parquet(CURRENT / "hospital_general.parquet")
    keep = set(gen.loc[gen["state"].isin(["RI", "DE", "VT"]), "facility_id"])
    for name in DATASETS:
        df = pd.read_parquet(CURRENT / f"{name}.parquet")
        p = {"rows": len(df), "columns": list(df.columns), "example": df.iloc[0].to_dict()}
        mcol = next((c for c in df.columns if c.endswith("measure_id")), None)
        if mcol:
            ncol = next((c for c in df.columns if c.endswith("measure_name") or c.endswith("question")), None)
            g = df.groupby(mcol)
            p["measures"] = {k: {"name": (v[ncol].iloc[0] if ncol else ""), "rows": len(v),
                                 "not_available": int((v.get("score", v.get("hcahps_answer_percent", pd.Series(dtype=str))) == "Not Available").sum())}
                             for k, v in g}
        prof[name] = p
        df[df["facility_id"].isin(keep)].to_csv(FIXTURES / f"{name}.csv", index=False)
    z = pd.read_parquet(CURRENT / "zip_centroids.parquet")
    zips = set(gen.loc[gen["facility_id"].isin(keep), "zip_code"].str[:5])
    z[z["zip"].str[:3].isin({x[:3] for x in zips})].to_csv(FIXTURES / "zip_centroids.csv", index=False)
    hist = sorted(HISTORY.glob("hospital_general_*.parquet"))
    for h in hist[-4:]:
        d = pd.read_parquet(h)
        d[d["facility_id"].isin(keep)].to_csv(FIXTURES / f"history_{h.stem.removeprefix('hospital_general_')}.csv", index=False)
    (REPORTS / "source_profile.json").write_text(json.dumps(prof, indent=1, default=str))
    print(f"profile written; fixtures for {len(keep)} hospitals", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--history", action="store_true")
    ap.add_argument("--profile", action="store_true")
    a = ap.parse_args()
    manifest = {"fetched_at": datetime.now(UTC).isoformat(timespec="seconds"), "datasets": fetch_current(),
                "zip_centroids": fetch_zcta()}
    if a.history:
        manifest["history"] = fetch_history()
    (RAW / "manifest.json").write_text(json.dumps(manifest, indent=1))
    if a.profile:
        profile(manifest)
    print("done", flush=True)


if __name__ == "__main__":
    sys.exit(main())
