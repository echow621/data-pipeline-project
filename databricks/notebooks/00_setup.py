# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: Simulated Source Tables + CDC Control Table
# MAGIC
# MAGIC Run this notebook **once** to create the demo environment. It mirrors what
# MAGIC `src/generate_source_data.py` does in the local (pandas) version of this
# MAGIC project, but writes Delta tables instead of a SQLite file.
# MAGIC
# MAGIC In a real setup this notebook would not exist -- the source tables would
# MAGIC already be there (e.g. synced from SAP S/4HANA or Salesforce via an
# MAGIC ingestion tool). It exists here purely so the pipeline is runnable end to
# MAGIC end without external systems.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "data_pipeline_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md ## Simulated source: `orders` and `logistics`
# MAGIC
# MAGIC Both tables carry an `updated_at` column -- this is the field the CDC
# MAGIC (watermark-based) load in the Bronze notebook relies on. Any source system
# MAGIC that exposes a reliable "last changed" timestamp (many REST APIs, most
# MAGIC relational sources) can be integrated the same way.

# COMMAND ----------

import random
from datetime import datetime, timedelta, timezone

from pyspark.sql import Row
import pyspark.sql.functions as F

random.seed(42)
customers = [f"CUST-{i:04d}" for i in range(1, 51)]
statuses = ["created", "processing", "shipped", "delivered", "cancelled"]
carriers = ["DHL", "DPD", "GLS", "Hermes"]


def now_iso(offset_minutes=0):
    return (datetime.now(timezone.utc) + timedelta(minutes=offset_minutes)).isoformat()


orders_rows, logistics_rows = [], []
for i in range(1, 201):
    order_id = f"ORD-{i:05d}"
    ts = now_iso(offset_minutes=-random.randint(60, 10000))
    orders_rows.append(
        Row(
            order_id=order_id,
            customer_id=random.choice(customers),
            order_status=random.choice(statuses),
            amount=round(random.uniform(15, 800), 2),
            created_at=ts,
            updated_at=ts,
        )
    )
    promised = random.randint(1, 5)
    actual = max(promised + random.choice([-1, 0, 0, 0, 1, 2]), 1)
    logistics_rows.append(
        Row(
            shipment_id=f"SHIP-{i:05d}",
            order_id=order_id,
            carrier=random.choice(carriers),
            promised_delivery_days=promised,
            actual_delivery_days=actual,
            created_at=ts,
            updated_at=ts,
        )
    )

spark.createDataFrame(orders_rows).write.mode("overwrite").saveAsTable("orders_source")
spark.createDataFrame(logistics_rows).write.mode("overwrite").saveAsTable("logistics_source")

print(f"Created {len(orders_rows)} orders and {len(logistics_rows)} logistics rows.")

# COMMAND ----------

# MAGIC %md ## CDC watermark control table
# MAGIC
# MAGIC Tracks, per source table, the maximum `updated_at` value already loaded
# MAGIC into Bronze. Equivalent to `logs/cdc_watermarks.json` in the local version,
# MAGIC just as a proper Delta table so it survives cluster restarts and can be
# MAGIC queried/audited like any other table.

# COMMAND ----------

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS cdc_watermarks (
        table_name STRING,
        watermark_value STRING,
        updated_at TIMESTAMP
    ) USING DELTA
    """
)

print("Setup complete.")

# COMMAND ----------

# MAGIC %md ## Optional: simulate new/changed records
# MAGIC
# MAGIC Run this cell (or re-run this notebook with `simulate_changes=true`) to add
# MAGIC new orders and update a few existing ones -- this is what the Bronze
# MAGIC notebook's incremental (CDC) load should pick up on the next pipeline run.

# COMMAND ----------

dbutils.widgets.dropdown("simulate_changes", "false", ["true", "false"])

if dbutils.widgets.get("simulate_changes") == "true":
    existing_count = spark.table("orders_source").count()
    new_rows = []
    for i in range(existing_count + 1, existing_count + 16):
        ts = now_iso()
        new_rows.append(
            Row(
                order_id=f"ORD-{i:05d}",
                customer_id=random.choice(customers),
                order_status="created",
                amount=round(random.uniform(15, 800), 2),
                created_at=ts,
                updated_at=ts,
            )
        )
    spark.createDataFrame(new_rows).write.mode("append").saveAsTable("orders_source")

    ids_to_update = [
        r.order_id
        for r in spark.table("orders_source").select("order_id").orderBy(F.rand()).limit(10).collect()
    ]
    for order_id in ids_to_update:
        spark.sql(
            f"""
            UPDATE orders_source
            SET order_status = '{random.choice(statuses)}', updated_at = '{now_iso()}'
            WHERE order_id = '{order_id}'
            """
        )

    print(f"Simulated {len(new_rows)} new orders and {len(ids_to_update)} updates.")
else:
    print("simulate_changes=false, skipped.")
