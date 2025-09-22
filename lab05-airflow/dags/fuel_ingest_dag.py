# ETL: ingest new Parquet files dropped into /opt/airflow/data every minute
# into Postgres with idempotent upserts. Files are archived after success
# and quarantined on failure. Watermark (latest processed filename) is stored
# in an Airflow Variable for exactly-once-ish behavior.
#
# Requires:
# - Airflow 2.4+ (tested on 2.11)
# - Airflow Connection "fuel_postgres" pointing at service "postgres"
# - pyarrow available in the Airflow image (see README or .env)
#
# Filename pattern expected: fuel_export_YYYYMMDD_HHMMSS.parquet

from __future__ import annotations

import os
import re
import json
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values, Json

import pyarrow.parquet as pq

DATA_DIR = Path("/opt/airflow/data")
ARCHIVE_DIR = DATA_DIR / "_archive"
BAD_DIR = DATA_DIR / "_bad"
GLOB_PATTERN = "fuel_export_*.parquet"
FILENAME_RE = re.compile(r"^fuel_export_(\d{8})_(\d{6})\.parquet$")

WATERMARK_VAR = "fuel_last_file"
PG_CONN_ID = os.environ.get("FUEL_PG_CONN_ID", "fuel_postgres")

DEFAULT_ARGS = {
    "owner": "ncs",
    "retries": 3,
    "retry_delay": 30,
}

with DAG(
    dag_id="fuel_ingest_minutely",
    start_date=datetime(2024, 1, 1),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["etl", "minutely"],
) as dag:

    @task
    def ensure_dirs() -> None:
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        BAD_DIR.mkdir(parents=True, exist_ok=True)
        logging.info("Ensured dirs: %s, %s", ARCHIVE_DIR, BAD_DIR)

    @task
    def ensure_table() -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS fuel_sales (
          transaction_id   text PRIMARY KEY,
          station_id       integer NOT NULL,
          dock             jsonb,
          ship_name        text,
          franchise        text,
          captain_name     text,
          species          text,
          fuel_type        text,
          fuel_units       double precision,
          price_per_unit   numeric(8,2),
          total_cost       numeric(12,2),
          services         text[],
          is_emergency     boolean,
          visited_at       timestamptz,
          arrival_date     date,
          coords_x         double precision,
          coords_y         double precision,
          source_file      text,
          loaded_at        timestamptz DEFAULT now()
        );
        """
        PostgresHook(postgres_conn_id=PG_CONN_ID).run(ddl)
        logging.info("Ensured table fuel_sales")

    @task
    def plan_files() -> List[str]:
        """
        Return a sorted list of absolute paths for *new* parquet files
        strictly after the stored watermark filename.
        """
        last = Variable.get(WATERMARK_VAR, default_var="")
        logging.info("Watermark: %r", last)

        candidates = []
        for p in DATA_DIR.glob(GLOB_PATTERN):
            if not FILENAME_RE.match(p.name):
                continue
            if last and p.name <= last:
                continue
            candidates.append(p)

        selected = [str(p) for p in sorted(candidates, key=lambda x: x.name)]
        logging.info("Planned files: %s", [Path(s).name for s in selected])
        return selected

    @task
    def _load_one(file_path: str) -> str:
        """
        Load a single parquet file into Postgres with upsert semantics.
        On success: move file to _archive and return its basename.
        On failure: move to _bad and re-raise.
        """
        p = Path(file_path)
        name = p.name
        logging.info("Loading %s", name)

        try:
            table = pq.read_table(p)
            rows = table.to_pylist()
            logging.info(
                "Read rows=%d cols=%d from %s", len(rows), len(table.schema), name
            )

            if not rows:
                shutil.move(str(p), ARCHIVE_DIR / name)
                logging.info("Empty file archived: %s", name)
                return name

            vals = []
            for r in rows:
                vals.append(
                    (
                        r["transaction_id"],
                        r["station_id"],
                        Json(r.get("dock") or {}),
                        r.get("ship_name"),
                        r.get("franchise"),
                        r.get("captain_name"),
                        r.get("species"),
                        r.get("fuel_type"),
                        float(r["fuel_units"])
                        if r.get("fuel_units") is not None
                        else None,
                        str(r["price_per_unit"])
                        if r.get("price_per_unit") is not None
                        else None,
                        str(r["total_cost"])
                        if r.get("total_cost") is not None
                        else None,
                        [str(s) for s in (r.get("services") or [])],
                        bool(r.get("is_emergency"))
                        if r.get("is_emergency") is not None
                        else None,
                        r.get("visited_at"),
                        r.get("arrival_date"),
                        float(r["coords_x"]) if r.get("coords_x") is not None else None,
                        float(r["coords_y"]) if r.get("coords_y") is not None else None,
                        name,
                    )
                )

            sql = """
                INSERT INTO fuel_sales (
                    transaction_id, station_id, dock, ship_name, franchise, captain_name, species,
                    fuel_type, fuel_units, price_per_unit, total_cost, services, is_emergency,
                    visited_at, arrival_date, coords_x, coords_y, source_file
                ) VALUES %s
                ON CONFLICT (transaction_id) DO NOTHING
            """

            hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    execute_values(cur, sql, vals, page_size=1000)
                conn.commit()

            shutil.move(str(p), ARCHIVE_DIR / name)
            logging.info("Loaded & archived: %s", name)
            return name

        except Exception:
            try:
                shutil.move(str(p), BAD_DIR / name)
                logging.exception("Load failed; moved to _bad: %s", name)
            except Exception:
                logging.exception("Load failed and could not move to _bad: %s", name)
            raise

    # map the loader across files planned this run
    loaded = _load_one.expand(file_path=plan_files())

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def advance_watermark(processed: List[str]) -> None:
        if not processed:
            logging.info("No new files; watermark unchanged.")
            return
        new_mark = max(processed)
        Variable.set(WATERMARK_VAR, new_mark)
        logging.info("Watermark advanced to %s", new_mark)

    ensure_dirs() >> ensure_table() >> loaded >> advance_watermark(loaded)
