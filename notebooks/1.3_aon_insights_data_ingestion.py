# Databricks notebook source

import hashlib
import re
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from aon_insights.config import get_env, load_config

# COMMAND ----------
# Create Spark session
spark = SparkSession.builder.getOrCreate()

# Load config
env = get_env(spark)
cfg = load_config("../project_config.yml", env)

CATALOG = cfg.catalog
SCHEMA = cfg.schema
TABLE_NAME = "aon_insights"

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
logger.info(f"Schema {CATALOG}.{SCHEMA} ready")

# COMMAND ----------
# Fetch Aon Insights by Topic from https://www.aon.com/en/insights
# Uses the underlying SearchStax Solr API for complete paginated results.
# Each insight's topics are concatenated from the source data (topic_tag_cf_sm)
# so no duplicates and no lost topic associations.

BASE_URL = "https://www.aon.com"
INSIGHTS_URL = f"{BASE_URL}/en/insights"

HTML_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

PAGE_SIZE = 50  # Solr rows per request


def extract_solr_config(soup: BeautifulSoup) -> dict:
    """
    Extract the SearchStax Solr URL and auth token from the
    page's embedded JavaScript CONFIGURATION block.

    Returns:
        Dict with keys 'url', 'auth_token', and 'headers'
    """
    for script in soup.find_all("script"):
        text = script.string or ""
        if "searchcloud" not in text.lower():
            continue

        url_match = re.search(r"url\s*:\s*[\"'](https://searchcloud[^\"']+)[\"']", text)
        token_match = re.search(r"authentication\s*:\s*[\"']([a-f0-9]{40})[\"']", text)

        if url_match and token_match:
            token = token_match.group(1)
            return {
                "url": url_match.group(1),
                "auth_token": token,
                "headers": {
                    "User-Agent": "Mozilla/5.0",
                    "Authorization": f"Token {token}",
                },
            }

    raise RuntimeError(
        "Could not extract Solr config from the Aon Insights page. "
        "The page structure may have changed."
    )


def _parse_solr_date(date_str: str | None) -> str | None:
    """Convert Solr datetime '2026-03-18T00:00:00Z' to 'YYYY-MM-DD'."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00")).strftime(
            "%Y-%m-%d"
        )
    except ValueError:
        return None


def fetch_all_aon_insights() -> list[dict]:
    """
    Fetch every Aon insight in a single paginated pass.
    Topics are extracted from each document's topic_tag_cf_sm field
    and concatenated with ', ' — so each URL appears exactly once.

    Returns:
        List of insight metadata dictionaries
    """
    # Fetch page once — reuse for config extraction
    print("Fetching Aon Insights page...")
    resp = requests.get(INSIGHTS_URL, headers=HTML_HEADERS, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    solr_cfg = extract_solr_config(soup)
    print(f"Solr endpoint: {solr_cfg['url']}")

    now = datetime.now().isoformat()
    insights = []
    seen_ids = set()
    start = 0

    while True:
        params = {
            "q": "*:*",
            "fq": "page_url_cf_s:\\/en\\/insights\\/*",
            "rows": PAGE_SIZE,
            "start": start,
            "sort": "publish_date_tdt desc",
            "fl": (
                "title_t,page_url_cf_s,"
                "content_type_tag_string_cf_s,"
                "topic_tag_cf_sm,"
                "publish_date_tdt,"
                "shortdescription_t"
            ),
            "wt": "json",
        }

        resp = requests.get(
            solr_cfg["url"],
            params=params,
            headers=solr_cfg["headers"],
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        docs = data["response"]["docs"]
        num_found = data["response"]["numFound"]

        if start == 0:
            print(f"Total insights in index: {num_found}")

        if not docs:
            break

        for doc in docs:
            page_url = doc.get("page_url_cf_s", "")
            title = doc.get("title_t", "")
            content_type = doc.get("content_type_tag_string_cf_s", "Unknown")

            if not page_url or not title:
                continue

            # Extract topic names from "guid|Name" entries
            raw_topics = doc.get("topic_tag_cf_sm", [])
            topic_names = [entry.split("|", 1)[1] for entry in raw_topics if "|" in entry]
            topics_str = (
                ", ".join(sorted(topic_names)) if topic_names else "Uncategorized"
            )

            description = doc.get("shortdescription_t", "")

            full_url = BASE_URL + page_url
            insights_id = hashlib.md5(full_url.encode()).hexdigest()[:12]

            if insights_id in seen_ids:
                continue
            seen_ids.add(insights_id)

            insights.append(
                {
                    "insights_id": insights_id,
                    "insights_name": title,
                    "description": description,
                    "insights_type": content_type,
                    "topic": topics_str,
                    "published_date": _parse_solr_date(doc.get("publish_date_tdt")),
                    "url": full_url,
                    "ingestion_timestamp": now,
                    "processed": None,
                    "volume_path": None,
                }
            )

        start += PAGE_SIZE
        if start >= num_found:
            break
        time.sleep(0.3)

    print(f"Total unique insights collected: {len(insights)}")
    return insights


# Run the scraper
insights = fetch_all_aon_insights()

# COMMAND ----------
# Create DataFrame

schema = StructType(
    [
        StructField("insights_id", StringType(), False),
        StructField("insights_name", StringType(), False),
        StructField("description", StringType(), True),
        StructField("insights_type", StringType(), False),
        StructField("topic", StringType(), False),
        StructField("published_date", StringType(), True),
        StructField("url", StringType(), False),
        StructField("ingestion_timestamp", StringType(), True),
        StructField("processed", StringType(), True),
        StructField("volume_path", StringType(), True),
    ]
)

df = spark.createDataFrame(insights, schema=schema)

# Write to Delta table using MERGE (upsert on insights_id)
table_path = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {table_path} (
        insights_id STRING NOT NULL,
        insights_name STRING,
        description STRING,
        insights_type STRING,
        topic STRING,
        published_date STRING,
        url STRING,
        ingestion_timestamp STRING,
        processed STRING,
        volume_path STRING
    )
""")

df.createOrReplaceTempView("new_insights")

spark.sql(f"""
    MERGE INTO {table_path} target
    USING new_insights source
    ON target.insights_id = source.insights_id
    WHEN MATCHED THEN UPDATE SET
        target.insights_name = source.insights_name,
        target.description = source.description,
        target.insights_type = source.insights_type,
        target.topic = source.topic,
        target.published_date = source.published_date,
        target.url = source.url,
        target.ingestion_timestamp = source.ingestion_timestamp
    WHEN NOT MATCHED THEN INSERT *
""")

logger.info(f"Merged into Delta table: {table_path}")
logger.info(f"Records: {spark.table(table_path).count()}")

# COMMAND ----------
# Verify the Data

# Read back the table
insights_df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE_NAME}")

logger.info(f"Table: {CATALOG}.{SCHEMA}.{TABLE_NAME}")
logger.info(f"Total insights: {insights_df.count()}")
logger.info("Schema:")
insights_df.printSchema()

logger.info("Sample records:")
insights_df.select("insights_id", "insights_name", "url", "published_date").show(
    5, truncate=60
)

# COMMAND ----------
# Data Statistics

logger.info("Total unique insights:")
insights_df.select("insights_id").distinct().count()

logger.info("Most recent ingestions:")
insights_df.select("insights_name", "url", "ingestion_timestamp").orderBy(
    "ingestion_timestamp", ascending=False
).show(5, truncate=60)
