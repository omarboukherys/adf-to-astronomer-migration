"""Generate the daily source files of a fictional online retailer.

The files play the role of the data that lands every day in the Azure Blob
container `landing`. They are loaded into Snowflake twice: by the legacy Azure
Data Factory pipelines and by the new Airflow (Astronomer) DAGs. Both loads are
compared to prove that the migration gives the same results.

Output (one file per entity and per day, header always present):
    data/landing/customers/customers_YYYY-MM-DD.csv
    data/landing/products/products_YYYY-MM-DD.csv
    data/landing/orders/orders_YYYY-MM-DD.csv
    data/landing/manifest.json   (injected edge cases + expected results)

The data is fully synthetic (no real person, no real company). The generator
only uses the Python standard library, so it runs anywhere, including inside
the Astro container with `astro dev pytest`.

Edge cases injected on purpose (all counted in manifest.json):
    - exact duplicate rows (a file re-sent twice)
    - order status updates arriving in later files (keep the latest version)
    - stale versions re-sent after a newer one (older updated_at)
    - invalid rows: negative quantity, impossible date
    - orders referencing a customer that never arrives (orphans)
    - customers arriving the day after their first order (late-arriving)
    - schema drift: column `channel` added to orders from 2026-10-15
    - days with no product change: header-only files
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path

START_DATE = date(2026, 10, 1)
NB_DAYS = 30
CHANNEL_FROM = date(2026, 10, 15)

CUSTOMER_COLUMNS = ["customer_id", "city", "segment", "signup_date", "updated_at"]
PRODUCT_COLUMNS = ["product_id", "product_name", "category", "unit_price", "updated_at"]
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "order_ts",
    "status",
    "updated_at",
]
ORDER_COLUMNS_WITH_CHANNEL = ORDER_COLUMNS + ["channel"]

CITIES = ["Paris", "Lyon", "Marseille", "Lille", "Bordeaux", "Nantes", "Toulouse", "Strasbourg"]
SEGMENTS = ["Standard", "Premium", "Business"]
CHANNELS = ["web", "mobile_app", "marketplace"]
CATEGORIES = {
    "Electronics": (49.0, 899.0),
    "Home": (9.0, 249.0),
    "Sports": (12.0, 199.0),
    "Beauty": (5.0, 89.0),
    "Books": (6.0, 45.0),
}
FINAL_STATUSES = ["DELIVERED", "DELIVERED", "DELIVERED", "DELIVERED", "CANCELLED", "RETURNED"]


@dataclass
class Counters:
    duplicate_rows: int = 0
    status_updates: int = 0
    stale_versions: int = 0
    negative_quantity: int = 0
    invalid_dates: int = 0
    orphan_orders: int = 0
    late_customers: int = 0
    header_only_files: list[str] = field(default_factory=list)


def ts(day: date, rng: random.Random) -> str:
    """Random timestamp within a day, as text."""
    moment = datetime(day.year, day.month, day.day) + timedelta(seconds=rng.randint(0, 86_399))
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: row.get(c, "") for c in columns})


def generate(output_dir: Path, seed: int = 2026, scale: int = 1) -> dict:
    """Write all files under output_dir and return the manifest."""
    rng = random.Random(seed)
    days = [START_DATE + timedelta(days=i) for i in range(NB_DAYS)]
    counters = Counters()

    customers_by_day: dict[date, list[dict]] = {d: [] for d in days}
    products_by_day: dict[date, list[dict]] = {d: [] for d in days}
    orders_by_day: dict[date, list[dict]] = {d: [] for d in days}

    # ---------- products: full load on day 1, a few price changes later ----------
    products: dict[str, dict] = {}
    for i in range(1, 80 * scale + 1):
        category = rng.choice(list(CATEGORIES))
        low, high = CATEGORIES[category]
        product = {
            "product_id": f"P{i:04d}",
            "product_name": f"{category} item {i:04d}",
            "category": category,
            "unit_price": f"{rng.uniform(low, high):.2f}",
            "updated_at": ts(days[0], rng),
        }
        products[product["product_id"]] = product
        products_by_day[days[0]].append(dict(product))
    for d in days[1:]:
        if rng.random() < 0.3:  # price changes on about 30 % of the days
            for pid in rng.sample(sorted(products), k=rng.randint(1, 4)):
                new = dict(products[pid])
                new["unit_price"] = f"{float(new['unit_price']) * rng.uniform(0.85, 1.10):.2f}"
                new["updated_at"] = ts(d, rng)
                products[pid] = new
                products_by_day[d].append(dict(new))

    # ---------- customers: initial base on day 1, new and changed customers later ----------
    customers: dict[str, dict] = {}
    next_customer = 1

    def new_customer(day: date) -> dict:
        nonlocal next_customer
        c = {
            "customer_id": f"C{next_customer:06d}",
            "city": rng.choice(CITIES),
            "segment": rng.choices(SEGMENTS, weights=[70, 20, 10])[0],
            "signup_date": day.isoformat(),
            "updated_at": ts(day, rng),
        }
        next_customer += 1
        customers[c["customer_id"]] = c
        return c

    for _ in range(1200 * scale):
        customers_by_day[days[0]].append(dict(new_customer(days[0])))

    late_customers: set[str] = set()
    for d in days[1:]:
        for _ in range(rng.randint(15, 30) * scale):
            c = new_customer(d)
            # about 1 new customer in 15 arrives in the next day's file (late-arriving)
            if d != days[-1] and rng.random() < 1 / 15:
                late_customers.add(c["customer_id"])
                customers_by_day[d + timedelta(days=1)].append(dict(c))
            else:
                customers_by_day[d].append(dict(c))
        # a few existing customers change city or segment (history to keep)
        for cid in rng.sample(sorted(customers), k=rng.randint(3, 8)):
            if cid in late_customers:
                continue
            changed = dict(customers[cid])
            if rng.random() < 0.5:
                changed["city"] = rng.choice([x for x in CITIES if x != changed["city"]])
            else:
                changed["segment"] = rng.choice([x for x in SEGMENTS if x != changed["segment"]])
            changed["updated_at"] = ts(d, rng)
            customers[cid] = changed
            customers_by_day[d].append(changed)
    counters.late_customers = len(late_customers)

    # ---------- orders: new orders + status updates in later files ----------
    latest_order: dict[str, dict] = {}  # final valid version of each order (expected result)
    invalid_orders: set[str] = set()
    next_order = 1

    for d in days:
        known = [cid for cid, c in customers.items() if c["signup_date"] <= d.isoformat()]
        for _ in range(rng.randint(150, 250) * scale):
            pid = rng.choice(sorted(products))
            order = {
                "order_id": f"O{next_order:07d}",
                "customer_id": rng.choice(known),
                "product_id": pid,
                "quantity": str(rng.choices([1, 2, 3, 4], weights=[60, 25, 10, 5])[0]),
                "unit_price": products[pid]["unit_price"],
                "order_ts": ts(d, rng),
                "status": "PLACED",
                "channel": rng.choice(CHANNELS),
            }
            order["updated_at"] = order["order_ts"]
            next_order += 1
            orders_by_day[d].append(order)
            latest_order[order["order_id"]] = order

            # status update 1 to 4 days later (if still inside the period)
            later = d + timedelta(days=rng.randint(1, 4))
            if later <= days[-1]:
                update = dict(order)
                update["status"] = rng.choice(FINAL_STATUSES)
                update["updated_at"] = ts(later, rng)
                orders_by_day[later].append(update)
                latest_order[order["order_id"]] = update
                counters.status_updates += 1
                # sometimes the old PLACED version is re-sent after the update (stale)
                stale_day = later + timedelta(days=1)
                if stale_day <= days[-1] and rng.random() < 0.01:
                    orders_by_day[stale_day].append(dict(order))
                    counters.stale_versions += 1

    # orders for late-arriving customers: placed the day before the customer file
    for cid in sorted(late_customers):
        d = date.fromisoformat(customers[cid]["signup_date"])
        pid = rng.choice(sorted(products))
        order = {
            "order_id": f"O{next_order:07d}",
            "customer_id": cid,
            "product_id": pid,
            "quantity": "1",
            "unit_price": products[pid]["unit_price"],
            "order_ts": ts(d, rng),
            "status": "PLACED",
            "channel": rng.choice(CHANNELS),
        }
        order["updated_at"] = order["order_ts"]
        next_order += 1
        orders_by_day[d].append(order)
        latest_order[order["order_id"]] = order

    # orphan orders: customer never sent
    for k in range(5):
        d = rng.choice(days)
        pid = rng.choice(sorted(products))
        order = {
            "order_id": f"O{next_order:07d}",
            "customer_id": f"C9{k:05d}",
            "product_id": pid,
            "quantity": "1",
            "unit_price": products[pid]["unit_price"],
            "order_ts": ts(d, rng),
            "status": "PLACED",
            "channel": rng.choice(CHANNELS),
        }
        order["updated_at"] = order["order_ts"]
        next_order += 1
        orders_by_day[d].append(order)
        latest_order[order["order_id"]] = order
        counters.orphan_orders += 1

    # invalid rows: negative quantity and impossible date (new order ids, to be rejected)
    for kind, n in (("negative_quantity", 3), ("invalid_date", 2)):
        for _ in range(n):
            d = rng.choice(days)
            pid = rng.choice(sorted(products))
            order = {
                "order_id": f"O{next_order:07d}",
                "customer_id": rng.choice(sorted(customers)),
                "product_id": pid,
                "quantity": "-2" if kind == "negative_quantity" else "1",
                "unit_price": products[pid]["unit_price"],
                "order_ts": "2026-13-45 10:00:00" if kind == "invalid_date" else ts(d, rng),
                "status": "PLACED",
                "channel": rng.choice(CHANNELS),
            }
            order["updated_at"] = ts(d, rng)
            next_order += 1
            orders_by_day[d].append(order)
            invalid_orders.add(order["order_id"])
            if kind == "negative_quantity":
                counters.negative_quantity += 1
            else:
                counters.invalid_dates += 1

    # exact duplicates: some rows written twice in the same file
    for _ in range(15):
        d = rng.choice(days)
        orders_by_day[d].append(dict(rng.choice(orders_by_day[d])))
        counters.duplicate_rows += 1

    # ---------- write files ----------
    landing = output_dir / "landing"
    for d in days:
        iso = d.isoformat()
        write_csv(landing / "customers" / f"customers_{iso}.csv", CUSTOMER_COLUMNS, customers_by_day[d])
        write_csv(landing / "products" / f"products_{iso}.csv", PRODUCT_COLUMNS, products_by_day[d])
        if not products_by_day[d]:
            counters.header_only_files.append(f"products/products_{iso}.csv")
        order_columns = ORDER_COLUMNS_WITH_CHANNEL if d >= CHANNEL_FROM else ORDER_COLUMNS
        rows = sorted(orders_by_day[d], key=lambda r: (r["updated_at"], r["order_id"]))
        write_csv(landing / "orders" / f"orders_{iso}.csv", order_columns, rows)

    # ---------- expected results (ground truth for the ADF vs Airflow comparison) ----------
    valid = {oid: o for oid, o in latest_order.items() if oid not in invalid_orders}
    status_counts: dict[str, int] = {}
    for o in valid.values():
        status_counts[o["status"]] = status_counts.get(o["status"], 0) + 1
    delivered_revenue = sum(
        int(o["quantity"]) * float(o["unit_price"]) for o in valid.values() if o["status"] == "DELIVERED"
    )

    manifest = {
        "seed": seed,
        "scale": scale,
        "period": {"start": days[0].isoformat(), "end": days[-1].isoformat(), "nb_days": NB_DAYS},
        "files_per_entity": NB_DAYS,
        "schema_drift": {"entity": "orders", "new_column": "channel", "from": CHANNEL_FROM.isoformat()},
        "edge_cases": {
            "duplicate_rows": counters.duplicate_rows,
            "status_updates": counters.status_updates,
            "stale_versions": counters.stale_versions,
            "negative_quantity": counters.negative_quantity,
            "invalid_dates": counters.invalid_dates,
            "orphan_orders": counters.orphan_orders,
            "late_customers": counters.late_customers,
            "header_only_product_files": len(counters.header_only_files),
        },
        "expected": {
            "customers": len(customers),
            "products": len(products),
            "valid_orders": len(valid),
            "rejected_orders": len(invalid_orders),
            "orders_by_final_status": dict(sorted(status_counts.items())),
            "delivered_revenue": round(delivered_revenue, 2),
        },
    }
    (landing / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", default="data", help="output folder (default: data)")
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--scale", type=int, default=1, help="multiply volumes")
    args = parser.parse_args()
    manifest = generate(Path(args.output), seed=args.seed, scale=args.scale)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
