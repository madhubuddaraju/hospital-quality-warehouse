import re
import requests
S = requests.Session()
S.headers["User-Agent"] = "Mozilla/5.0"
page = S.get("https://data.cms.gov/provider-data/archived-data/hospitals", timeout=60).text
srcs = re.findall(r'src="([^"]+\.js)"', page)
print("scripts:", srcs[:20])
for s in srcs:
    url = s if s.startswith("http") else "https://data.cms.gov" + s
    js = S.get(url, timeout=60).text
    hits = sorted(set(re.findall(r'["\'`]([^"\'`]{0,80}archiv[^"\'`]{0,80})["\'`]', js, re.I)))
    if hits:
        print("==", url)
        for h in hits[:40]:
            print("  ", h)
for u in ["https://data.cms.gov/provider-data/api/1/pdc/archive/hospitals",
          "https://data.cms.gov/provider-data/api/1/pdc/topics/hospitals/archive",
          "https://data.cms.gov/provider-data/api/1/archive/hospitals",
          "https://data.cms.gov/provider-data/sites/default/files/archive/Hospitals/2025/hospitals_01_2025.zip",
          "https://data.cms.gov/provider-data/sites/default/files/archive/Hospitals/current/hospitals_current_data.zip"]:
    r = S.get(u, timeout=60, stream=True)
    print(r.status_code, r.headers.get("content-type"), r.headers.get("content-length"), u)
    if "json" in (r.headers.get("content-type") or ""):
        print(r.text[:1500])
    r.close()
