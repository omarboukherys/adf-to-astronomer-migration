"""dbt transformations of the retail data, run by Astronomer Cosmos.

Cosmos turns every dbt model into an Airflow task, each followed by its tests,
so a failing test stops the downstream models and is visible in the Airflow UI.

Scheduling is data-aware (Airflow 3 Assets): the DAG runs as soon as the loading
DAG `load_landing_to_snowflake` has updated the three raw tables. There is no
fixed time to keep in sync with the load, unlike a chain of ADF triggers.

Models (include/dbt/retail):
  staging  (views, schema STAGING): typed versions of customers, products, orders
  marts    (tables, schema MARTS) : dim_customers, dim_products, fct_orders,
                                    audit_rejected_orders, mart_daily_sales
A reconciliation test checks the marts against the figures computed from the
source files (customers, products, valid and rejected orders, delivered revenue).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import Asset
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestIndirectSelection

# Paths inside the Astro containers (overridable to run the tests elsewhere).
DBT_PROJECT_PATH = Path(os.getenv("DBT_PROJECT_PATH", "/usr/local/airflow/include/dbt/retail"))
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/usr/local/airflow/dbt_venv/bin/dbt")

# Raw tables published by the loading DAG.
RAW_ASSETS = [Asset(f"retail.raw_airflow.{entity}") for entity in ("customers", "products", "orders")]

dbt_retail = DbtDag(
    dag_id="dbt_retail",
    project_config=ProjectConfig(DBT_PROJECT_PATH),
    profile_config=ProfileConfig(
        profile_name="retail",
        target_name="dev",
        profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
    ),
    execution_config=ExecutionConfig(
        dbt_executable_path=DBT_EXECUTABLE_PATH,
        # a model's test task runs only the tests that depend on that model alone...
        test_indirect_selection=TestIndirectSelection.CAUTIOUS,
    ),
    # ...and tests spanning several models (relationships, reconciliation) get their
    # own task, which waits for all the models they read.
    render_config=RenderConfig(should_detach_multiple_parents_tests=True),
    schedule=RAW_ASSETS,  # runs when the three raw tables have been refreshed
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["transformation", "dbt", "snowflake"],
    doc_md=__doc__,
)
