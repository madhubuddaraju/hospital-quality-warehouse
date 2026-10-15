import re
import requests
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0"
js = S.get("https://data.cms.gov/provider-data/js/index.js", timeout=60).text
for m in re.finditer(r"archive/aggregate", js):
    print("CTX:", js[max(0, m.start() - 300): m.end() + 120].replace("\n", " "))
base = ["https://data.cms.gov/provider-data/api/1/pdc", "https://data.cms.gov/provider-data/api/1", "https://data.cms.gov/provider-data"]
for b in base:
    for t in ["hospitals", "Hospitals"]:
        for suffix in [f"/archive/aggregate/theme/{t}/relative", "/archive/aggregate/current/theme/all/relative"]:
            u = b + suffix
            r = S.get(u, timeout=60)
            print(r.status_code, r.headers.get("content-type"), u)
            if r.ok and "json" in (r.headers.get("content-type") or ""):
                print(r.text[:3000])
