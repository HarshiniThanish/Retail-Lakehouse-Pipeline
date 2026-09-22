"""
cdc_loader.py
---------------------------------------------------------------------------
Cursor-based CDC extraction from the operational Postgres source into
Bronze Delta tables, with idempotent upserts.

This module is invoked from the Databricks ingestion job (triggered by
Airflow's `ingest_cdc` task). It is written to run inside a Databricks
notebook/job cluster where `spark` is available; when run standalone it
falls back to a local pandas + psycopg2 path for testing.

Strategy:
  1. For each source table, read the last successful cursor value
     (max `updated_at`) from a Delta control table.
  2. Pull all rows from Postgres where `updated_at` > cursor.
  3. MERGE (upsert) into the Bronze Delta table on primary key.
  4. Advance the cursor.
---------------------------------------------------------------------------
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import psycopg2
import psycopg2.extras

PG_CONFIG = dict(
    host=os.environ.get("PG_HOST", "localhost"),
    port=os.environ.get("PG_PORT", "5432"),
    dbname=os.environ.get("PG_DB", "walmart_ops"),
    user=os.environ.get("PG_USER", "walmart_app"),
    password=os.environ.get("PG_PASSWORD", "changeme"),
)


@dataclass
class TableConfig:
    source_table: str
    primary_key: str
    cursor_column: str = "updated_at"
    bronze_table: str = ""  # defaults to f"bronze.{source_table}"

    def __post_init__(self):
        if not self.bronze_table:
            self.bronze_table = f"bronze.{self.source_table}"


# One entry per operational entity ingested from the Ghost Postgres source.
TABLES = [
    TableConfig("customers", primary_key="customer_id"),
    TableConfig("stores", primary_key="store_id"),
    TableConfig("products", primary_key="product_id"),
    TableConfig("employees", primary_key="employee_id"),
    TableConfig("orders", primary_key="order_id"),
    TableConfig("order_items", primary_key="order_item_id"),
]


def get_last_cursor(bronze_table: str, cursor_column: str) -> Optional[datetime]:
    """
    Reads the max(cursor_column) already landed in Bronze.
    Replace this stub with a real Delta table read, e.g.:
        spark.table(bronze_table).agg(F.max(cursor_column)).collect()[0][0]
    """
    # Local/testing fallback: no prior watermark -> full backfill.
    return None


def extract_incremental(cfg: TableConfig, since: Optional[datetime]):
    """Pulls rows changed since the last cursor from the Postgres source."""
    query = f"SELECT * FROM {cfg.source_table}"
    params = []
    if since is not None:
        query += f" WHERE {cfg.cursor_column} > %s"
        params.append(since)
    query += f" ORDER BY {cfg.cursor_column} ASC"

    with psycopg2.connect(**PG_CONFIG) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
    return rows


def upsert_to_bronze(cfg: TableConfig, rows: list[dict]) -> int:
    """
    Idempotent upsert into the Bronze Delta table.
    Replace with a real Delta MERGE, e.g.:

        from delta.tables import DeltaTable
        target = DeltaTable.forName(spark, cfg.bronze_table)
        (target.alias("t")
            .merge(source_df.alias("s"), f"t.{cfg.primary_key} = s.{cfg.primary_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
    """
    if not rows:
        print(f"[cdc_loader] {cfg.source_table}: no new/changed rows")
        return 0
    print(f"[cdc_loader] {cfg.source_table}: upserting {len(rows)} rows into {cfg.bronze_table}")
    # ... Delta MERGE call goes here in the real Databricks environment ...
    return len(rows)


def run() -> None:
    total = 0
    for cfg in TABLES:
        since = get_last_cursor(cfg.bronze_table, cfg.cursor_column)
        rows = extract_incremental(cfg, since)
        total += upsert_to_bronze(cfg, rows)
    print(f"[cdc_loader] ingestion complete: {total} rows upserted across {len(TABLES)} tables")


if __name__ == "__main__":
    run()
