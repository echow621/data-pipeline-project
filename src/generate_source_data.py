"""
Simulates a source system (e.g. what an SAP/CRM extract would look like):
two tables, `orders` and `logistics`, each with a `updated_at` timestamp
column used later as the CDC watermark.

Run without arguments to create the initial database.
Run with --simulate-changes to insert new rows and update a few existing
ones, so the pipeline's incremental (CDC) load has something to pick up.
"""
import argparse
import random
import sqlite3
from datetime import datetime, timedelta, timezone

from config import RAW_DB_PATH

CUSTOMERS = [f"CUST-{i:04d}" for i in range(1, 51)]
STATUSES = ["created", "processing", "shipped", "delivered", "cancelled"]
CARRIERS = ["DHL", "DPD", "GLS", "Hermes"]


def _connect():
    conn = sqlite3.connect(RAW_DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            customer_id TEXT,
            order_status TEXT,
            amount REAL,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS logistics (
            shipment_id TEXT PRIMARY KEY,
            order_id TEXT,
            carrier TEXT,
            promised_delivery_days INTEGER,
            actual_delivery_days INTEGER,
            created_at TEXT,
            updated_at TEXT
        )
        """
    )
    return conn


def _now(offset_minutes=0):
    return (datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)).isoformat()


def seed_initial_data(conn, n_orders=200):
    orders, shipments = [], []
    for i in range(1, n_orders + 1):
        order_id = f"ORD-{i:05d}"
        ts = _now(offset_minutes=-random.randint(60, 10000))
        orders.append(
            (
                order_id,
                random.choice(CUSTOMERS),
                random.choice(STATUSES),
                round(random.uniform(15, 800), 2),
                ts,
                ts,
            )
        )
        promised = random.randint(1, 5)
        actual = promised + random.choice([-1, 0, 0, 0, 1, 2])
        shipments.append(
            (
                f"SHIP-{i:05d}",
                order_id,
                random.choice(CARRIERS),
                promised,
                max(actual, 1),
                ts,
                ts,
            )
        )

    conn.executemany(
        "INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?)", orders
    )
    conn.executemany(
        "INSERT OR REPLACE INTO logistics VALUES (?,?,?,?,?,?,?)", shipments
    )
    conn.commit()
    print(f"Seeded {len(orders)} orders and {len(shipments)} shipments.")


def simulate_changes(conn, n_new=15, n_updates=10):
    cur = conn.cursor()

    # New orders arriving (simulates new records since last CDC run)
    existing = cur.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    new_rows = []
    for i in range(existing + 1, existing + 1 + n_new):
        order_id = f"ORD-{i:05d}"
        ts = _now()
        new_rows.append(
            (order_id, random.choice(CUSTOMERS), "created",
             round(random.uniform(15, 800), 2), ts, ts)
        )
    cur.executemany("INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?,?)", new_rows)

    # Existing orders changing status (simulates updates since last CDC run)
    ids = [r[0] for r in cur.execute(
        "SELECT order_id FROM orders ORDER BY RANDOM() LIMIT ?", (n_updates,)
    ).fetchall()]
    for order_id in ids:
        cur.execute(
            "UPDATE orders SET order_status = ?, updated_at = ? WHERE order_id = ?",
            (random.choice(STATUSES), _now(), order_id),
        )

    conn.commit()
    print(f"Simulated {n_new} new orders and {n_updates} updated orders.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--simulate-changes", action="store_true")
    args = parser.parse_args()

    connection = _connect()
    if args.simulate_changes:
        simulate_changes(connection)
    else:
        seed_initial_data(connection)
    connection.close()
