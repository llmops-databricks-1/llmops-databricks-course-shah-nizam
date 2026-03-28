"""Check all available fields in the Solr index for Aon Insights."""

import re

import requests
from bs4 import BeautifulSoup

resp = requests.get(
    "https://www.aon.com/en/insights",
    headers={"User-Agent": "Mozilla/5.0"},
    timeout=30,
)
soup = BeautifulSoup(resp.text, "html.parser")

# Extract Solr config
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

if not solr_url:
    print("Could not extract Solr config")
    exit(1)

# Fetch one doc with ALL fields
params = {
    "q": "*:*",
    "fq": r"page_url_cf_s:\/en\/insights\/*",
    "rows": 1,
    "fl": "*",
    "wt": "json",
}
r = requests.get(
    solr_url,
    params=params,
    headers={"Authorization": f"Token {token}"},
    timeout=30,
)
doc = r.json()["response"]["docs"][0]

print("All available fields:")
for k, v in sorted(doc.items()):
    val_preview = str(v)[:150]
    print(f"  {k}: {val_preview}")
