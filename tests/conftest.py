"""Test settings applied before any DAG is imported."""

import os

# Cosmos caches the dbt project listing in an Airflow Variable (metadata database).
# Tests parse the DAGs without a database, so the cache is turned off here.
os.environ.setdefault("AIRFLOW__COSMOS__ENABLE_CACHE_DBT_LS", "False")
