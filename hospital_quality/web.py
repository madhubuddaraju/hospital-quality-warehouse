"""Build docs/index.html (the hospital chooser) and docs/lineage.html (dbt docs) from the warehouse marts."""

import json
import shutil
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
GEO = ROOT / "dashboard" / "us_states.json"


def _rows(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def payload(warehouse: Path, target: Path, results_path: Path) -> dict:
    con = duckdb.connect(str(warehouse), read_only=True)
    catalog = _rows(con, "select measure_id, label, domain, reasons, lower_is_better, unit from reference.measure_catalog")
    mids = [m["measure_id"] for m in catalog]
    hospitals = _rows(con, """select facility_id, facility_name, city, state, zip5, round(lat, 3) lat, round(lon, 3) lon,
                                     overall_rating, hospital_type, has_emergency_services, ed_volume, phone, location_source,
                                     ownership = 'Veterans Administration' as is_va
                              from marts.dim_hospitals where lat is not null order by facility_id""")
    index = {h["facility_id"]: i for i, h in enumerate(hospitals)}
    values = [[None] * len(mids) for _ in hospitals]
    flags = [["-"] * len(mids) for _ in hospitals]
    code = {"better": "b", "worse": "w", "no_different": "n"}
    for r in con.execute("select facility_id, measure_id, score, compared_to_national from marts.fct_hospital_measures").fetchall():
        if r[0] in index:
            j = mids.index(r[1])
            values[index[r[0]]][j] = round(r[2], 2)
            flags[index[r[0]]][j] = code.get(r[3], "-")
    medians = dict(con.execute("select measure_id, any_value(national_median) from marts.fct_hospital_measures group by 1").fetchall())
    scores = {}
    for f, reason, score, worse, better in con.execute(
            "select facility_id, reason, score, measures_worse_than_national, measures_better_than_national "
            "from marts.mart_hospital_reason_scores").fetchall():
        if f in index:
            scores.setdefault(reason, []).append([index[f], score, worse, better])
    zips = ";".join(f"{z},{lat:.2f},{lon:.2f}" for z, lat, lon in
                    con.execute("select zip5, lat, lon from staging.stg_census__zip_centroids order by zip5").fetchall())
    rel_file = ROOT / "data" / "snapshots" / "releases.json"
    release_dates = json.loads(rel_file.read_text()) if rel_file.exists() else []
    con.execute("create or replace temp table releases as select cast(unnest(?) as date) as d", [release_dates or ["1900-01-01"]])
    hist = _rows(con, """
        with releases as (select d from releases),
        held as (
            select r.d, h.overall_rating
            from releases r join marts.mart_rating_history h
              on h.valid_from <= r.d and (h.valid_to is null or h.valid_to > r.d))
        select d as release, count(*) filter (where overall_rating = 1) r1, count(*) filter (where overall_rating = 2) r2,
               count(*) filter (where overall_rating = 3) r3, count(*) filter (where overall_rating = 4) r4,
               count(*) filter (where overall_rating = 5) r5, count(*) filter (where overall_rating is null) unrated
        from held group by d order by d""")
    moves = _rows(con, """select valid_from as release,
                                 count(*) filter (where overall_rating > previous_rating) up,
                                 count(*) filter (where overall_rating < previous_rating) down
                          from marts.mart_rating_history where previous_rating is not null group by 1 order by 1""")
    watch = _rows(con, """select w.facility_id, w.facility_name, w.city, w.state, w.overall_rating, w.outcomes_worse,
                                 w.worse_measures, w.previous_rating, w.rating_dropped_on, w.reason
                          from marts.mart_watch_list w order by w.outcomes_worse desc, w.rating_dropped_on desc nulls last""")
    states = _rows(con, "select * from marts.mart_state_summary order by state")
    reasons = _rows(con, "select reason, label from reference.care_reasons")
    modified = con.execute("select max(cms_modified) from marts.dim_hospitals").fetchone()[0]
    con.close()

    results = json.loads(results_path.read_text()) if results_path.exists() else {"results": []}
    manifest = json.loads((target / "manifest.json").read_text()) if (target / "manifest.json").exists() else {"nodes": {}}
    kinds = {}
    for n in manifest["nodes"].values():
        kinds[n["resource_type"]] = kinds.get(n["resource_type"], 0) + 1
    statuses = {}
    for r in results["results"]:
        statuses[r["status"]] = statuses.get(r["status"], 0) + 1
    pipeline = {"nodes": kinds, "last_build": statuses, "built_at": results.get("metadata", {}).get("generated_at", "")}
    for d in (hist, moves, watch):
        for r in d:
            for k, v in r.items():
                if hasattr(v, "isoformat"):
                    r[k] = v.isoformat()
    return {"catalog": catalog, "medians": medians, "hospitals": hospitals, "values": values,
            "flags": ["".join(f) for f in flags], "scores": scores, "reasons": reasons, "zips": zips,
            "history": hist, "moves": moves, "watch": watch, "states": states,
            "geo": json.loads(GEO.read_text()) if GEO.exists() else {}, "pipeline": pipeline,
            "cms_modified": modified.isoformat() if modified else ""}


def build(warehouse: Path, target: Path, results_path: Path) -> dict:
    data = payload(warehouse, target, results_path)
    DOCS.mkdir(exist_ok=True)
    page = (ROOT / "dashboard" / "template.html").read_text()
    page = page.replace("__DATA__", json.dumps(data, separators=(",", ":"), default=str).replace("</", "<\\/"))
    (DOCS / "index.html").write_text(page)
    static = target / "static_index.html"
    if static.exists():
        shutil.copy(static, DOCS / "lineage.html")
    return {"hospitals": len(data["hospitals"]), "watch_list": len(data["watch"]), "releases": len(data["history"]),
            "page_mb": round((DOCS / "index.html").stat().st_size / 1e6, 2)}
