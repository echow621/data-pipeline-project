# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer: Incremental Extract (CDC)
# MAGIC
# MAGIC PySpark/Delta equivalent of `src/bronze_layer.py` in the local project.
# MAGIC Same watermark-based CDC principle: read from the control table what the
# MAGIC last-loaded `updated_at` was, pull only newer rows from the source, and
# MAGIC **upsert** them into the Bronze Delta table with `MERGE INTO` (instead of
# MAGIC the pandas `concat` + `drop_duplicates` used locally).
# MAGIC
# MAGIC `MERGE INTO` is the standard Databricks pattern for CDC-style upserts and
# MAGIC is worth calling out explicitly in an interview -- it does in one
# MAGIC declarative statement what the local project does with two pandas calls.

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "data_pipeline_demo", "Schema")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# MAGIC %run ./_shared_utils

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_TABLES = {
    "orders": {"source": "orders_source", "bronze": "orders_bronze", "key": "order_id"},
    "logistics": {"source": "logistics_source", "bronze": "logistics_bronze", "key": "shipment_id"},
}

# COMMAND ----------

results = {}

for logical_name, cfg in SOURCE_TABLES.items():
    watermark = get_watermark(logical_name)

    source_df = spark.table(cfg["source"])
    if watermark:
        incremental_df = source_df.filter(F.col("updated_at") > F.lit(watermark))
    else:
        incremental_df = source_df  # first run: full load

    row_count = incremental_df.count()
    results[logical_name] = row_count

    if row_count > 0:
        bronze_table = cfg["bronze"]
        key_col = cfg["key"]

        if spark.catalog.tableExists(bronze_table):
            # Upsert: new rows inserted, changed rows overwritten
            incremental_df.createOrReplaceTempView("incremental_updates")
            spark.sql(
                f"""
                MERGE INTO {bronze_table} AS target
                USING incremental_updates AS source
                ON target.{key_col} = source.{key_col}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
        else:
            incremental_df.write.format("delta").saveAsTable(bronze_table)

        new_watermark = incremental_df.agg(F.max("updated_at")).collect()[0][0]
        save_watermark(logical_name, new_watermark)

    print(f"{logical_name}: {row_count} new/changed rows loaded into Bronze")
    log("bronze", logical_name, row_count, "ok")

# COMMAND ----------

# MAGIC %md
# MAGIC `results` (row counts per table) is returned via `dbutils.notebook.exit`.
# MAGIC Each table's row count is also written to the `pipeline_log` Delta table
# MAGIC above, the same way Silver and Gold do -- so a single query against
# MAGIC `pipeline_log` shows the full bronze -> silver -> gold run history.

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(results))
