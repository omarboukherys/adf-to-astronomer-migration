"""Tests of the source file generator (standard library only)."""

import csv
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))

from generate_source_files import (  # noqa: E402
    CHANNEL_FROM,
    NB_DAYS,
    ORDER_COLUMNS,
    ORDER_COLUMNS_WITH_CHANNEL,
    generate,
)


@pytest.fixture(scope="module")
def output(tmp_path_factory):
    out = tmp_path_factory.mktemp("data")
    manifest = generate(out, seed=2026)
    return out / "landing", manifest


def read(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames, list(reader)


def test_one_file_per_entity_and_day(output):
    landing, _ = output
    for entity in ("customers", "products", "orders"):
        assert len(list((landing / entity).glob(f"{entity}_*.csv"))) == NB_DAYS


def test_same_seed_gives_same_files(tmp_path, output):
    landing, _ = output
    generate(tmp_path, seed=2026)
    for entity in ("customers", "products", "orders"):
        for f in (landing / entity).glob("*.csv"):
            assert f.read_bytes() == (tmp_path / "landing" / entity / f.name).read_bytes()


def test_schema_drift_on_orders(output):
    landing, _ = output
    for f in (landing / "orders").glob("orders_*.csv"):
        header, _ = read(f)
        day = f.stem.removeprefix("orders_")
        expected = ORDER_COLUMNS_WITH_CHANNEL if day >= CHANNEL_FROM.isoformat() else ORDER_COLUMNS
        assert header == expected


def test_header_only_product_files_exist(output):
    landing, manifest = output
    empty = [f for f in (landing / "products").glob("*.csv") if not read(f)[1]]
    assert len(empty) == manifest["edge_cases"]["header_only_product_files"] > 0


def test_edge_cases_are_present(output):
    _, manifest = output
    cases = manifest["edge_cases"]
    for name in ("duplicate_rows", "status_updates", "negative_quantity", "invalid_dates", "orphan_orders", "late_customers"):
        assert cases[name] > 0, name


def test_duplicate_rows_found_in_files(output):
    landing, manifest = output
    duplicates = 0
    for f in (landing / "orders").glob("*.csv"):
        rows = [tuple(r.values()) for r in read(f)[1]]
        duplicates += len(rows) - len(set(rows))
    assert duplicates == manifest["edge_cases"]["duplicate_rows"]


def test_expected_results_match_files(output):
    """Recompute the expected results from the files: latest version per order, invalid rows excluded."""
    landing, manifest = output
    latest: dict[str, dict] = {}
    for f in sorted((landing / "orders").glob("*.csv")):
        for r in read(f)[1]:
            current = latest.get(r["order_id"])
            if current is None or r["updated_at"] > current["updated_at"]:
                latest[r["order_id"]] = r

    def is_valid(r: dict) -> bool:
        return int(r["quantity"]) > 0 and r["order_ts"][5:7] <= "12"

    valid = {k: v for k, v in latest.items() if is_valid(v)}
    expected = manifest["expected"]
    assert len(valid) == expected["valid_orders"]
    assert len(latest) - len(valid) == expected["rejected_orders"]
    revenue = sum(int(r["quantity"]) * float(r["unit_price"]) for r in valid.values() if r["status"] == "DELIVERED")
    assert round(revenue, 2) == expected["delivered_revenue"]


def test_manifest_written(output):
    landing, manifest = output
    assert json.loads((landing / "manifest.json").read_text(encoding="utf-8")) == manifest
