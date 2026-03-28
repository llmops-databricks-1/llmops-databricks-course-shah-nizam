# Databricks notebook source

from datetime import datetime
import hashlib
import re

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
# Fetch Aon Insights from https://www.aon.com/en/insights using BeautifulSoup

BASE_URL = "https://www.aon.com"
INSIGHTS_URL = f"{BASE_URL}/en/insights"


def fetch_aon_insights(url: str = INSIGHTS_URL) -> list[dict]:
    """
    Scrape report titles and URLs from the Aon Insights page.

    Args:
        url: The Aon Insights page URL

    Returns:
        List of insight metadata dictionaries
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }

    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    now = datetime.now().isoformat()

    insights = []
    seen_urls = set()

    # Look for article/report links on the insights page
    for link in soup.find_all("a", href=True):
        href = link["href"]
        title = link.get_text(strip=True)

        # Filter for insight/report links and skip empty titles
        if not title or len(title) < 5:
            continue

        # Build full URL
        if href.startswith("/"):
            full_url = BASE_URL + href
        elif href.startswith("http"):
            full_url = href
        else:
            continue

        # Only keep links under /en/insights/ that look like report pages
        if "/en/insights/" not in full_url:
            continue

        # Deduplicate by URL
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

        # Generate a stable ID from the URL
        insights_id = hashlib.md5(full_url.encode()).hexdigest()[:12]

        # Try to extract a date/time from the page or link context
        time_tag = link.find_parent().find("time") if link.find_parent() else None
        time_value = time_tag.get("datetime", time_tag.get_text(strip=True)) if time_tag else None

        insights.append({
            "insights_id": insights_id,
            "insights_name": title,
            "url": full_url,
            "time": time_value,
            "ingestion_timestamp": now,
        })

    return insights


logger.info("Fetching Aon Insights...")
insights = fetch_aon_insights()
logger.info(f"Fetched {len(insights)} insights")

if insights:
    logger.info("Sample insight:")
    logger.info(f"  Name: {insights[0]['insights_name']}")
    logger.info(f"  URL:  {insights[0]['url']}")
    logger.info(f"  ID:   {insights[0]['insights_id']}")

# COMMAND ----------
# Create Delta Table in Unity Catalog
# Store the Aon Insights metadata in a Delta table for downstream processing.

# Define schema
schema = StructType([
    StructField("insights_id", StringType(), False),
    StructField("insights_name", StringType(), False),
    StructField("url", StringType(), False),
    StructField("time", StringType(), True),
    StructField("ingestion_timestamp", StringType(), True),
])

# Create DataFrame
df = spark.createDataFrame(insights, schema=schema)

# Write to Delta table
table_path = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(table_path)

logger.info(f"Created Delta table: {table_path}")
logger.info(f"Records: {df.count()}")

# COMMAND ----------
# Verify the Data

# Read back the table
insights_df = spark.table(f"{CATALOG}.{SCHEMA}.{TABLE_NAME}")

logger.info(f"Table: {CATALOG}.{SCHEMA}.{TABLE_NAME}")
logger.info(f"Total insights: {insights_df.count()}")
logger.info("Schema:")
insights_df.printSchema()

logger.info("Sample records:")
insights_df.select("insights_id", "insights_name", "url", "time").show(5, truncate=60)

# COMMAND ----------
# Data Statistics

logger.info("Total unique insights:")
insights_df.select("insights_id").distinct().count()

logger.info("Most recent ingestions:")
insights_df.select("insights_name", "url", "ingestion_timestamp") \
    .orderBy("ingestion_timestamp", ascending=False) \
    .show(5, truncate=60)
