"""Tests of the dbt DAG rendered by Cosmos (no Snowflake call)."""

import pytest
from airflow.dag_processing.dagbag import DagBag

DAG_ID = "dbt_retail"
# Models with at least one test of their own (Cosmos then creates <model>.run and <model>.test).
TESTED_MODELS = [
    "stg_customers",
    "stg_products",
    "stg_orders",
    "dim_customers",
    "dim_products",
    "fct_orders",
    "audit_rejected_orders",
]


@pytest.fixture(scope="module")
def dag():
    dagbag = DagBag(dag_folder="dags")
    assert not dagbag.import_errors, dagbag.import_errors
    return dagbag.dags[DAG_ID]


def test_one_run_and_test_task_per_model(dag):
    task_ids = {task.task_id for task in dag.tasks}
    for model in TESTED_MODELS:
        assert f"{model}.run" in task_ids
        assert f"{model}.test" in task_ids
    assert "mart_daily_sales_run" in task_ids  # no test of its own: a single task


def test_models_follow_their_dependencies(dag):
    assert "stg_orders.test" in dag.get_task("fct_orders.run").upstream_task_ids
    assert "fct_orders.test" in dag.get_task("mart_daily_sales_run").upstream_task_ids


def test_cross_model_tests_wait_for_all_their_models(dag):
    reconciliation = next(t for t in dag.tasks if "assert_marts_match_source_manifest" in t.task_id)
    upstream = reconciliation.upstream_task_ids
    for model in ("dim_customers", "dim_products", "fct_orders", "audit_rejected_orders"):
        assert f"{model}.test" in upstream


def test_scheduled_on_the_raw_assets(dag):
    schedule = repr(dag.timetable.asset_condition)
    for entity in ("customers", "products", "orders"):
        assert f"retail.raw_airflow.{entity}" in schedule


def test_retries(dag):
    for task in dag.tasks:
        assert task.retries == 2
