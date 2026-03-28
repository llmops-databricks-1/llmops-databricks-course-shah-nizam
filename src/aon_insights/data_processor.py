"""
aon_insights table (URLs)
   ↓ (download_and_store_html)
HTML in Volume + aon_insights table updated (processed, volume_path)
   ↓ (parse_html_content)
aon_parsed_html_table (extracted text)
   ↓ (process_chunks)
aon_chunks_table (clean text + metadata)
   ↓ (VectorSearchManager - separate class) (2.4 notebook)
Vector Search Index (embeddings)
"""

import json
import os
import re
import time

import requests
from bs4 import BeautifulSoup
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    explode,
    lit,
    udf,
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from aon_insights.config import ProjectConfig

HTML_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


class DataProcessor:
    """
    DataProcessor handles the complete workflow of:
    - Reading insight URLs from the aon_insights table
    - Downloading HTML content with BeautifulSoup
    - Storing HTML files in a Unity Catalog Volume
    - Extracting and cleaning text chunks
    - Saving chunks to Delta tables
    """

    def __init__(self, spark: SparkSession, config: ProjectConfig) -> None:
        """
        Initialize DataProcessor with Spark session and configuration.

        Args:
            spark: SparkSession instance
            config: ProjectConfig object with table configurations
        """
        self.spark = spark
        self.cfg = config
        self.catalog = config.catalog
        self.schema = config.schema
        self.volume = config.volume

        self.end = time.strftime("%Y%m%d%H%M", time.gmtime())
        self.html_dir = f"/Volumes/{self.catalog}/{self.schema}/{self.volume}/{self.end}"
        self.insights_table = f"{self.catalog}.{self.schema}.aon_insights"
        self.parsed_table = f"{self.catalog}.{self.schema}.aon_parsed_html_table"

    def download_and_store_html(self) -> list[dict] | None:
        """
        Read unprocessed URLs from aon_insights table,
        fetch HTML content using BeautifulSoup, and store
        HTML files in the Volume.

        Returns:
            List of processed insight records, or None if nothing to process
        """
        # Get unprocessed insights (where processed is NULL)
        unprocessed_df = self.spark.sql(f"""
            SELECT insights_id, insights_name, url
            FROM {self.insights_table}
            WHERE processed IS NULL
        """)

        rows = unprocessed_df.collect()

        if len(rows) == 0:
            logger.info("No unprocessed insights found.")
            return None

        logger.info(f"Found {len(rows)} unprocessed insights to download.")

        # Create Volume directory for this batch
        os.makedirs(self.html_dir, exist_ok=True)

        records = []
        for row in rows:
            insights_id = row["insights_id"]
            url = row["url"]
            insights_name = row["insights_name"]

            try:
                resp = requests.get(url, headers=HTML_HEADERS, timeout=30)
                resp.raise_for_status()

                # Parse with BeautifulSoup to get clean HTML
                soup = BeautifulSoup(resp.text, "html.parser")

                # Save HTML to Volume
                html_path = f"{self.html_dir}/{insights_id}.html"
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(str(soup))

                records.append({
                    "insights_id": insights_id,
                    "volume_path": html_path,
                })

                logger.info(f"Downloaded: {insights_name}")

            except Exception as e:
                logger.warning(
                    f"Failed to download {insights_id} ({url}): {e}"
                )

            # Avoid hitting rate limits
            time.sleep(1)

        if len(records) == 0:
            logger.info("No HTML files were successfully downloaded.")
            return None

        logger.info(
            f"Downloaded {len(records)} HTML files to {self.html_dir}"
        )

        # Update aon_insights table with processed timestamp and volume_path
        update_schema = T.StructType([
            T.StructField("insights_id", T.StringType(), False),
            T.StructField("volume_path", T.StringType(), True),
        ])

        update_df = (
            self.spark.createDataFrame(records, schema=update_schema)
            .withColumn("processed", lit(self.end))
        )

        update_df.createOrReplaceTempView("processed_insights")
        self.spark.sql(f"""
            MERGE INTO {self.insights_table} target
            USING processed_insights source
            ON target.insights_id = source.insights_id
            WHEN MATCHED THEN UPDATE SET
                target.processed = source.processed,
                target.volume_path = source.volume_path
        """)

        logger.info(
            f"Updated {len(records)} records in {self.insights_table}"
        )
        return records

    def parse_html_content(self) -> None:
        """
        Parse stored HTML files to extract text content
        and store in parsed HTML table.
        """
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.parsed_table} (
                insights_id STRING,
                path STRING,
                parsed_content STRING,
                processed STRING
            )
        """)

        # Read HTML files from Volume
        processed_df = self.spark.sql(f"""
            SELECT insights_id, volume_path
            FROM {self.insights_table}
            WHERE processed = '{self.end}'
              AND volume_path IS NOT NULL
        """)

        rows = processed_df.collect()

        if len(rows) == 0:
            logger.info("No HTML files to parse for this batch.")
            return

        parsed_records = []
        for row in rows:
            insights_id = row["insights_id"]
            volume_path = row["volume_path"]

            try:
                with open(volume_path, "r", encoding="utf-8") as f:
                    html_content = f.read()

                soup = BeautifulSoup(html_content, "html.parser")

                # Extract from article body sections only (column-content),
                # skipping carousels, sidebar, navigation, etc.
                content_sections = soup.find_all(
                    "section", class_="column-content"
                )

                elements = []
                for section in content_sections:
                    # Skip disclaimer/legal boilerplate
                    if section.find(
                        "div", class_="disclaimer-block__block"
                    ):
                        continue
                    for idx, tag in enumerate(
                        section.find_all(
                            ["h1", "h2", "h3", "h4", "p", "li"]
                        )
                    ):
                        text = tag.get_text(strip=True)
                        if text and len(text) > 10:
                            elements.append({
                                "type": "text",
                                "id": str(len(elements)),
                                "content": text,
                                "tag": tag.name,
                            })

                parsed_json = json.dumps({"document": {"elements": elements}})

                parsed_records.append({
                    "insights_id": insights_id,
                    "path": volume_path,
                    "parsed_content": parsed_json,
                    "processed": self.end,
                })

            except Exception as e:
                logger.warning(f"Failed to parse {volume_path}: {e}")

        if parsed_records:
            schema = T.StructType([
                T.StructField("insights_id", T.StringType(), False),
                T.StructField("path", T.StringType(), True),
                T.StructField("parsed_content", T.StringType(), True),
                T.StructField("processed", T.StringType(), True),
            ])

            parsed_df = self.spark.createDataFrame(
                parsed_records, schema=schema
            )
            parsed_df.write.mode("append").saveAsTable(self.parsed_table)
            logger.info(
                f"Parsed {len(parsed_records)} HTML files "
                f"and saved to {self.parsed_table}"
            )

    @staticmethod
    def _extract_chunks(
        parsed_content_json: str,
        max_chunk_chars: int = 2000,
        overlap_chars: int = 200,
    ) -> list[tuple[str, str]]:
        """
        Extract chunks from parsed_content JSON, merging adjacent
        text elements into larger chunks of approximately max_chunk_chars.

        Args:
            parsed_content_json: JSON string containing parsed document structure
            max_chunk_chars: Target maximum characters per chunk (~512 tokens)
            overlap_chars: Character overlap between consecutive chunks

        Returns:
            List of tuples containing (chunk_id, content)
        """
        parsed_dict = json.loads(parsed_content_json)
        elements = parsed_dict.get("document", {}).get("elements", [])

        # Collect all text content in order
        texts = []
        for element in elements:
            if element.get("type") == "text":
                content = element.get("content", "").strip()
                if content:
                    texts.append(content)

        if not texts:
            return []

        # Merge into larger chunks
        chunks = []
        current_chunk = []
        current_len = 0

        for text in texts:
            text_len = len(text)

            # If adding this text exceeds the limit, finalize the current chunk
            if current_len > 0 and current_len + text_len + 1 > max_chunk_chars:
                chunk_text = "\n".join(current_chunk)
                chunks.append((str(len(chunks)), chunk_text))

                # Start next chunk with overlap from the end of the previous
                overlap_text = chunk_text[-overlap_chars:] if len(chunk_text) > overlap_chars else ""
                current_chunk = [overlap_text, text] if overlap_text else [text]
                current_len = len(overlap_text) + text_len + 1
            else:
                current_chunk.append(text)
                current_len += text_len + 1

        # Don't forget the last chunk
        if current_chunk:
            chunks.append((str(len(chunks)), "\n".join(current_chunk)))

        return chunks

    @staticmethod
    def _clean_chunk(text: str) -> str:
        """
        Clean and normalize chunk text.

        Args:
            text: Raw text content

        Returns:
            Cleaned text content
        """
        # Fix hyphenation across line breaks
        t = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)

        # Collapse internal newlines into spaces
        t = re.sub(r"\s*\n\s*", " ", t)

        # Collapse repeated whitespace
        t = re.sub(r"\s+", " ", t)

        return t.strip()

    def process_chunks(self) -> None:
        """
        Process parsed documents to extract and clean chunks.
        Reads from parsed HTML table and saves to aon_chunks_table.
        """
        logger.info(
            f"Processing parsed documents from "
            f"{self.parsed_table} for batch {self.end}"
        )

        df = self.spark.table(self.parsed_table).where(
            f"processed = '{self.end}'"
        )

        # Define schema for the extracted chunks
        chunk_schema = ArrayType(
            StructType([
                StructField("chunk_id", StringType(), True),
                StructField("content", StringType(), True),
            ])
        )

        extract_chunks_udf = udf(self._extract_chunks, chunk_schema)
        clean_chunk_udf = udf(self._clean_chunk, StringType())

        # Get metadata from aon_insights table
        metadata_df = self.spark.table(self.insights_table).select(
            col("insights_id"),
            col("insights_name"),
            col("insights_type"),
            col("topic"),
            col("published_date"),
            col("url"),
        )

        # Create the transformed dataframe
        chunks_df = (
            df.withColumn(
                "chunks", extract_chunks_udf(col("parsed_content"))
            )
            .withColumn("chunk", explode(col("chunks")))
            .select(
                col("insights_id"),
                col("chunk.chunk_id").alias("chunk_id"),
                clean_chunk_udf(col("chunk.content")).alias("text"),
                concat_ws(
                    "_", col("insights_id"), col("chunk.chunk_id")
                ).alias("id"),
            )
            .join(metadata_df, "insights_id", "left")
        )

        # Write to table
        aon_chunks_table = (
            f"{self.catalog}.{self.schema}.aon_chunks_table"
        )
        chunks_df.write.mode("append").saveAsTable(aon_chunks_table)
        logger.info(f"Saved chunks to {aon_chunks_table}")

        # Enable Change Data Feed
        self.spark.sql(f"""
            ALTER TABLE {aon_chunks_table}
            SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
        """)
        logger.info(f"Change Data Feed enabled for {aon_chunks_table}")

    def process_and_save(self) -> None:
        """
        Complete workflow: download HTML, parse content, and process chunks.
        """
        # Step 1: Download HTML from insight URLs and store in Volume
        records = self.download_and_store_html()

        if records is None:
            logger.info("No new insights to process. Exiting.")
            return

        # Step 2: Parse HTML content to extract text
        self.parse_html_content()
        logger.info("Parsed HTML documents.")

        # Step 3: Process chunks
        self.process_chunks()
        logger.info("Processing complete!")
