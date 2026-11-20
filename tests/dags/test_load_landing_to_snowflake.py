"""Tests of the loading DAG: structure, scheduling and rendered SQL (no Snowflake call)."""

import pendulum
import pytest
from airflow.dag_processing.dagbag import DagBag

DAG_ID = "load_landing_to_snowflake"


@pytest.fixture(scope="module")
def dag():
    dagbag = DagBag(dag_folder="dags")
    assert not dagbag.import_errors, dagbag.import_errors
    return dagbag.dags[DAG_ID]


def render(dag, text, day):
    context = {"data_interval_start": pendulum.datetime(*day, tz="UTC")}
    return dag.get_template_env().from_string(text).render(**context)


def test_one_sensor_and_one_load_per_entity(dag):
    assert sorted(t.task_id for t in dag.tasks) == sorted(
        f"{entity}.{step}"
        for entity in ("customers", "products", "orders")
        for step in ("wait_for_file", "load_file")
    )
    for entity in ("customers", "products", "orders"):
        assert dag.get_task(f"{entity}.load_file").upstream_task_ids == {f"{entity}.wait_for_file"}


def test_daily_intervals_over_october(dag):
    restriction = type(
        "Restriction",
        (),
        {
            "earliest": pendulum.datetime(2026, 10, 1, tz="UTC"),
            "latest": pendulum.datetime(2026, 10, 30, tz="UTC"),
            "catchup": True,
        },
    )()
    info = dag.timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
    assert info.data_interval.start == pendulum.datetime(2026, 10, 1, tz="UTC")
    assert info.data_interval.end == pendulum.datetime(2026, 10, 2, tz="UTC")
    assert dag.max_active_runs == 5


def test_load_is_idempotent_and_traceable(dag):
    task = dag.get_task("orders.load_file")
    assert task.autocommit is False  # DELETE and COPY in one transaction
    delete, copy = (render(dag, statement, (2026, 10, 15)) for statement in task.sql)
    assert delete.strip() == (
        "DELETE FROM RETAIL.RAW_AIRFLOW.ORDERS WHERE _source_file = 'orders/orders_2026-10-15.csv'"
    )
    assert "FILES = ('orders_2026-10-15.csv')" in copy
    assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in copy
    assert "_source_file = METADATA$FILENAME" in copy
    assert "FORCE = TRUE" in copy


def test_sensor_checks_the_file_of_the_day(dag):
    sensor = dag.get_task("products.wait_for_file")
    assert render(dag, sensor.sql, (2026, 10, 3)) == (
        "LIST @RETAIL.LANDING.AZURE_LANDING/products/products_2026-10-03.csv"
    )
    assert sensor.mode == "reschedule"


def test_each_load_publishes_its_asset(dag):
    for entity in ("customers", "products", "orders"):
        outlets = dag.get_task(f"{entity}.load_file").outlets
        assert [asset.name for asset in outlets] == [f"retail.raw_airflow.{entity}"]


def test_retries_like_adf(dag):
    for task in dag.tasks:
        assert task.retries == 2
