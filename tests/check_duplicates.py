"""Check for duplicate insights_id in Solr results."""

import hashlib
import re

import requests
from bs4 import BeautifulSoup

resp = requests.get(
    "https://www.aon.com/en/insights",
    headers={"User-Agent": "Mozilla/5.0"},
    timeout=30,
)
soup = BeautifulSoup(resp.text, "html.parser")

solr_url = None
token = None
for script in soup.find_all("script"):
    text = script.string or ""
    if "searchcloud" not in text.lower():
        continue
    url_m = re.search(r"url\s*:\s*[\"'](https://searchcloud[^\"']+)[\"']", text)
    tok_m = re.search(r"authentication\s*:\s*[\"']([a-f0-9]{40})[\"']", text)
    if url_m and tok_m:
        solr_url = url_m.group(1)
        token = tok_m.group(1)
        break

BASE_URL = "https://www.aon.com"
TARGET_ID = "b7d4a9645bfc"

# Fetch all docs and find which ones produce this hash
start = 0
PAGE_SIZE = 50
matches = []

while True:
    params = {
        "q": "*:*",
        "fq": r"page_url_cf_s:\/en\/insights\/*",
        "rows": PAGE_SIZE,
        "start": start,
        "sort": "publish_date_tdt desc",
        "fl": "title_t,page_url_cf_s,publish_date_tdt,content_type_tag_string_cf_s",
        "wt": "json",
    }
    r = requests.get(
        solr_url,
        params=params,
        headers={"Authorization": f"Token {token}"},
        timeout=30,
    )
    data = r.json()
    docs = data["response"]["docs"]
    num_found = data["response"]["numFound"]

    if not docs:
        break

    for doc in docs:
        page_url = doc.get("page_url_cf_s", "")
        title = doc.get("title_t", "")
        full_url = BASE_URL + page_url
        iid = hashlib.md5(full_url.encode()).hexdigest()[:12]
        if iid == TARGET_ID:
            matches.append(
                {
                    "title": title,
                    "url": full_url,
                    "page_url_raw": page_url,
                    "published": doc.get("publish_date_tdt"),
                    "type": doc.get("content_type_tag_string_cf_s"),
                }
            )

    start += PAGE_SIZE
    if start >= num_found:
        break

print(f"Total docs in Solr: {num_found}")
print(f"Docs matching insights_id={TARGET_ID}: {len(matches)}\n")
for i, m in enumerate(matches):
    print(f"Match {i + 1}:")
    for k, v in m.items():
        print(f"  {k}: {v}")
    print()
