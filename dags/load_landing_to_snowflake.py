"""Daily load of the landing files (Azure Blob) into Snowflake RAW_AIRFLOW.

Airflow 3 replacement of the ADF pipeline `pl_load_daily_landing`
(see docs/adf-legacy.md and docs/migration-mapping.md).

For each day and each entity (customers, products, orders):
  1. wait_for_file : wait until the file is visible in the Snowflake external stage
                     (the stage reads Azure Blob through a storage integration, so
                     Airflow holds no Azure credential);
  2. load_file     : in one transaction, delete the rows of that file, then
                     COPY INTO the raw table by column name, with the source file
                     name and load time on every row.

Improvements over the ADF pipeline:
  - idempotent: re-running a day replaces its rows instead of duplicating them
    (ADF sends FORCE = TRUE without cleaning up first);
  - traceable: _source_file and _loaded_at are filled (ADF direct copy cannot);
  - columns are matched by name (ADF maps by position);
  - each table is published as an Airflow Asset, which triggers the dbt DAG.

Scheduling mirrors the ADF tumbling window trigger: one run per daily interval,
the date loaded is the start of the interval (= ADF windowStartTime).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.sdk import DAG, Asset, TaskGroup
from airflow.timetables.interval import CronDataIntervalTimetable

SNOWFLAKE_CONN_ID = "snowflake_default"
STAGE = "@RETAIL.LANDING.AZURE_LANDING"
ENTITIES = ["customers", "products", "orders"]

# Date of the data to load: start of the daily interval (ADF: windowStartTime).
LOAD_DATE = "{{ data_interval_start | ds }}"

# One Asset per raw table: the dbt DAG is scheduled on them.
RAW_ASSETS = {entity: Asset(f"retail.raw_airflow.{entity}") for entity in ENTITIES}

with DAG(
    dag_id="load_landing_to_snowflake",
    description="Azure Blob (landing) -> Snowflake RAW_AIRFLOW, one run per day",
    schedule=CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    start_date=datetime(2026, 10, 1),
    end_date=datetime(2026, 10, 30),  # last interval: 30/10 -> 31/10 (as ADF)
    catchup=False,  # history is loaded with an explicit backfill
    max_active_runs=5,  # same as maxConcurrency of the ADF trigger
    default_args={
        "owner": "data-engineering",
        "retries": 2,  # same as the ADF Copy activity policy
        "retry_delay": timedelta(minutes=1),
    },
    tags=["ingestion", "snowflake", "migrated-from-adf"],
    doc_md=__doc__,
) as dag:
    for entity in ENTITIES:
        file_name = f"{entity}_{LOAD_DATE}.csv"
        source_file = f"{entity}/{file_name}"  # value of METADATA$FILENAME
        table = f"RETAIL.RAW_AIRFLOW.{entity.upper()}"

        with TaskGroup(group_id=entity):
            wait_for_file = SqlSensor(
                task_id="wait_for_file",
                conn_id=SNOWFLAKE_CONN_ID,
                sql=f"LIST {STAGE}/{source_file}",
                mode="reschedule",  # frees the worker slot between checks
                poke_interval=300,
                timeout=6 * 60 * 60,
            )

            load_file = SQLExecuteQueryOperator(
                task_id="load_file",
                conn_id=SNOWFLAKE_CONN_ID,
                autocommit=False,  # DELETE + COPY in a single transaction
                sql=[
                    f"DELETE FROM {table} WHERE _source_file = '{source_file}'",
                    f"""
                    COPY INTO {table}
                    FROM {STAGE}/{entity}/
                    FILES = ('{file_name}')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    INCLUDE_METADATA = (
                        _source_file = METADATA$FILENAME,
                        _loaded_at = METADATA$START_SCAN_TIME
                    )
                    ON_ERROR = ABORT_STATEMENT
                    FORCE = TRUE
                    """,
                ],
                outlets=[RAW_ASSETS[entity]],
            )

            wait_for_file >> load_file
