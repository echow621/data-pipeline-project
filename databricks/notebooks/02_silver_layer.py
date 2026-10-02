# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Layer: Clean & Harmonize
# MAGIC
# MAGIC PySpark equivalent of `src/silver_layer.py`. Same rules, translated from
# MAGIC pandas to Spark DataFrame operations.

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

# COMMAND ----------

def clean_orders(df):
    df = df.dropDuplicates(["order_id"])
    df = df.withColumn("amount", F.col("amount").cast("double"))
    df = df.dropna(subset=["order_id", "customer_id", "amount"])
    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    df = df.withColumn("updated_at", F.to_timestamp("updated_at"))
    df = df.withColumn("order_status", F.trim(F.lower(F.col("order_status"))))
    return df


def clean_logistics(df):
    df = df.dropDuplicates(["shipment_id"])
    df = df.withColumn("promised_delivery_days", F.col("promised_delivery_days").cast("int"))
    df = df.withColumn("actual_delivery_days", F.col("actual_delivery_days").cast("int"))
    df = df.dropna(subset=["shipment_id", "order_id"])
    df = df.withColumn("created_at", F.to_timestamp("created_at"))
    df = df.withColumn("updated_at", F.to_timestamp("updated_at"))
    df = df.withColumn("carrier", F.trim(F.upper(F.col("carrier"))))
    return df

# COMMAND ----------

results = {}

if spark.catalog.tableExists("orders_bronze"):
    orders_silver = clean_orders(spark.table("orders_bronze"))
    orders_silver.write.format("delta").mode("overwrite").saveAsTable("orders_silver")
    results["orders"] = orders_silver.count()
    log("silver", "orders", results["orders"], "ok")

if spark.catalog.tableExists("logistics_bronze"):
    logistics_silver = clean_logistics(spark.table("logistics_bronze"))
    logistics_silver.write.format("delta").mode("overwrite").saveAsTable("logistics_silver")
    results["logistics"] = logistics_silver.count()
    log("silver", "logistics", results["logistics"], "ok")

print(results)

# COMMAND ----------

# MAGIC %md
# MAGIC Note: Silver is written with `mode("overwrite")` here for simplicity,
# MAGIC since Bronze already holds the deduplicated, upserted state after the
# MAGIC `MERGE INTO` in the Bronze notebook. In a larger project Silver could
# MAGIC also be merged incrementally the same way Bronze is.

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps(results))
