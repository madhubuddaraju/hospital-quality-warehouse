"""Dagster orchestration for the hospital quality warehouse.

    dagster dev -m hospital_quality.definitions                                  # local UI with the asset graph
    dagster asset materialize -m hospital_quality.definitions --select '*'      # what the weekly GitHub Action runs

Asset graph:
    cms_raw_data ─┬─> dbt models (staging -> intermediate -> marts, every dbt test is an asset check) ─> dashboard
    cms_release_history ─> hospital_rating_snapshot (replays every past release through the dbt snapshot) ─┘
"""

import importlib.util
import json
import os
import shutil
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    asset_check,
    define_asset_job,
    in_process_executor,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = ROOT / "dbt"
RAW = ROOT / "data" / "raw"
SNAPSHOTS = ROOT / "data" / "snapshots"
WAREHOUSE = ROOT / "data" / "warehouse.duckdb"


def _script(name: str):
    """Load scripts/<name>.py as a module (works in Dagster's step subprocesses too)."""
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


os.environ.setdefault("DUCKDB_PATH", str(WAREHOUSE))
os.environ.setdefault("RAW_DIR", str(RAW / "current"))
os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))

dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()
SNAPSHOT_KEY = AssetKey(["history", "hospital_rating_snapshot"])   # dagster-dbt keys nodes by schema/name


# ----------------------------------------------------------------------------- ingestion

RAW_TABLES = ["hospital_general", "unplanned_visits", "complications_deaths", "timely_care", "patient_survey", "infections",
              "zip_centroids"]
RAW_KEYS = {t: AssetKey(["raw", t]) for t in RAW_TABLES}


@multi_asset(group_name="ingest", specs=[AssetSpec(k, kinds={"python", "parquet"}) for k in RAW_KEYS.values()],
             description="Current Medicare hospital datasets and Census ZIP centroids, as parquet.")
def cms_raw_data(context: AssetExecutionContext):
    if os.environ.get("USE_FIXTURES") == "1":
        _script("use_fixtures")                     # loads the committed three-state sample
        manifest = {"datasets": {}}
    else:
        fetch_cms = _script("fetch_cms")
        manifest = {"fetched_at": datetime.now(UTC).isoformat(timespec="seconds"), "datasets": fetch_cms.fetch_current(),
                    "zip_centroids": fetch_cms.fetch_zcta()}
        (RAW / "manifest.json").write_text(json.dumps(manifest, indent=1))
    for t, key in RAW_KEYS.items():
        info = manifest["datasets"].get(t, {})
        rows = info.get("rows") or len(pd.read_parquet(RAW / "current" / f"{t}.parquet"))
        yield MaterializeResult(asset_key=key, metadata={"rows": rows, "cms_modified": info.get("modified") or ""})


@asset_check(asset=RAW_KEYS["hospital_general"],
             description="CMS refreshes quarterly; data older than 120 days means a release was missed.")
def cms_data_is_fresh() -> AssetCheckResult:
    m = json.loads((RAW / "manifest.json").read_text()) if (RAW / "manifest.json").exists() else {}
    dates = [v.get("modified") for v in m.get("datasets", {}).values() if v.get("modified")]
    if not dates:
        return AssetCheckResult(passed=True, metadata={"note": "fixture run, no release date"})
    age = (date.today() - date.fromisoformat(max(dates))).days
    return AssetCheckResult(passed=age <= 120, severity=AssetCheckSeverity.WARN, metadata={"days_since_release": age})


@asset(group_name="ingest", kinds={"python"}, description="Hospital General Information from every past CMS release since 2021.")
def cms_release_history(context: AssetExecutionContext) -> MaterializeResult:
    if os.environ.get("USE_FIXTURES") == "1":
        _script("use_fixtures")
    else:
        fetch_cms = _script("fetch_cms")
        fetch_cms.fetch_history()
    files = sorted((RAW / "history").glob("hospital_general_*.parquet"))
    return MaterializeResult(metadata={"releases": len(files), "first": files[0].stem[-10:] if files else "",
                                       "last": files[-1].stem[-10:] if files else ""})


# ----------------------------------------------------------------------------- rating history (dbt snapshot replay)

def _align(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Older releases lack newer columns (e.g. the birthing-friendly flag): add them empty, keep today's layout."""
    for c in columns:
        if c not in df.columns:
            df[c] = ""
    return df[columns]


@asset(key=SNAPSHOT_KEY, group_name="history", kinds={"dbt", "duckdb"}, deps=[RAW_KEYS["hospital_general"], cms_release_history],
       description="Type 2 history of hospital ratings: each past release replayed through the dbt snapshot, oldest first.")
def hospital_rating_snapshot(context: AssetExecutionContext, dbt: DbtCliResource) -> MaterializeResult:
    SNAPSHOTS.mkdir(parents=True, exist_ok=True)
    store, done_file = SNAPSHOTS / "hospital_rating_snapshot.parquet", SNAPSHOTS / "releases.json"
    done = set(json.loads(done_file.read_text())) if done_file.exists() else set()
    with duckdb.connect(str(WAREHOUSE)) as con:                 # restore history kept in git between runs
        con.execute("create schema if not exists history")
        if store.exists():
            con.execute(f"create or replace table history.hospital_rating_snapshot as select * from read_parquet('{store}')")
        else:
            con.execute("drop table if exists history.hospital_rating_snapshot")
    current = pd.read_parquet(RAW / "current" / "hospital_general.parquet")
    cur_release = str(current["_cms_modified"].iloc[0]) if "_cms_modified" in current else "current"
    releases = sorted((RAW / "history").glob("hospital_general_*.parquet"))
    todo = [(p.stem.removeprefix("hospital_general_"), p) for p in releases]
    todo = [(d, p) for d, p in todo if d not in done and d < cur_release] + [(cur_release, None)]
    raw_dir = os.environ["RAW_DIR"]
    with tempfile.TemporaryDirectory() as tmp:
        for release, path in todo:
            if path is not None:
                _align(pd.read_parquet(path), list(current.columns)).assign(_cms_modified=release).to_parquet(
                    Path(tmp) / "hospital_general.parquet", index=False)
                os.environ["RAW_DIR"] = tmp
            else:
                os.environ["RAW_DIR"] = raw_dir
            dbt.cli(["run", "--select", "stg_cms__hospitals"]).wait()
            dbt.cli(["snapshot", "--vars", json.dumps({"snapshot_at": release})]).wait()
            done.add(release)
            context.log.info(f"replayed release {release}")
    os.environ["RAW_DIR"] = raw_dir
    dbt.cli(["run", "--select", "stg_cms__hospitals"]).wait()
    with duckdb.connect(str(WAREHOUSE)) as con:
        con.execute(f"copy history.hospital_rating_snapshot to '{store}' (format parquet)")
        n, changes = con.execute("select count(distinct facility_id), count(*) - count(distinct facility_id) "
                                 "from history.hospital_rating_snapshot").fetchone()
    done_file.write_text(json.dumps(sorted(done), indent=1))
    return MaterializeResult(metadata={"hospitals": n, "recorded_changes": changes, "releases_replayed": len(done)})


# ----------------------------------------------------------------------------- dbt models and tests

class Translator(DagsterDbtTranslator):
    """Each raw CMS and Census source is one of the assets produced by cms_raw_data."""

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return RAW_KEYS[dbt_resource_props["name"]]
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, exclude="hospital_rating_snapshot", dagster_dbt_translator=Translator())
def warehouse(context: AssetExecutionContext, dbt: DbtCliResource):
    run = dbt.cli(["build", "--exclude", "hospital_rating_snapshot"], context=context)
    yield from run.stream()
    # keep the build's test results where the dashboard can report them (each dbt call has its own target folder)
    (DBT_DIR / "target").mkdir(exist_ok=True)
    shutil.copy(run.target_path / "run_results.json", DBT_DIR / "target" / "build_results.json")


# ----------------------------------------------------------------------------- dashboard

@asset(group_name="publish", kinds={"python", "html"},
       deps=[get_asset_key_for_model([warehouse], m) for m in
             ("dim_hospitals", "fct_hospital_measures", "mart_hospital_reason_scores", "mart_state_summary",
              "mart_watch_list", "mart_rating_history")],
       description="docs/index.html (hospital chooser) and docs/lineage.html (dbt docs with the lineage graph).")
def dashboard(context: AssetExecutionContext, dbt: DbtCliResource) -> MaterializeResult:
    from hospital_quality import web
    docs = dbt.cli(["docs", "generate", "--static"]).wait()
    out = web.build(WAREHOUSE, docs.target_path, DBT_DIR / "target" / "build_results.json")
    return MaterializeResult(metadata={k: v for k, v in out.items() if isinstance(v, (int, float, str))})


@asset_check(asset=dashboard, description="The chooser needs most hospitals on the map and scored for a general stay.")
def chooser_coverage() -> AssetCheckResult:
    with duckdb.connect(str(WAREHOUSE), read_only=True) as con:
        located, total = con.execute("select count(lat), count(*) from marts.dim_hospitals").fetchone()
        scored = con.execute("select count(*) from marts.mart_hospital_reason_scores where reason = 'general'").fetchone()[0]
    return AssetCheckResult(passed=located / total >= 0.95,
                            metadata={"located_share": round(located / total, 3), "scored_general": scored, "hospitals": total})


# DuckDB allows one writer at a time, so steps run one after another in a single process.
weekly_refresh = define_asset_job("weekly_refresh", selection="*", executor_def=in_process_executor)

defs = Definitions(
    assets=[cms_raw_data, cms_release_history, hospital_rating_snapshot, warehouse, dashboard],
    asset_checks=[cms_data_is_fresh, chooser_coverage],
    jobs=[weekly_refresh],
    schedules=[ScheduleDefinition(job=weekly_refresh, cron_schedule="0 6 * * 1", execution_timezone="America/New_York")],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
    executor=in_process_executor,
)


def clean_warehouse() -> None:
    """Start from an empty DuckDB file (the snapshot history lives in data/snapshots, not in the warehouse)."""
    if WAREHOUSE.exists():
        WAREHOUSE.unlink()
    shutil.rmtree(DBT_DIR / "target", ignore_errors=True)
