# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Layer: KPIs, Data Quality & Run Log
# MAGIC
# MAGIC PySpark equivalent of `src/gold_layer.py` + `src/data_quality.py`. Also
# MAGIC writes the pipeline run log as a Delta table (`pipeline_log`) -- the
# MAGIC Databricks counterpart to `logs/pipeline_log.csv` locally. In practice
# MAGIC you'd also rely on the Databricks Job run history / Lakehouse Monitoring
# MAGIC for observability; this table is kept for parity with the local version
# MAGIC and for easy querying from a BI tool.

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

# MAGIC %md ## Gold KPIs

# COMMAND ----------

gold_results = {}

if spark.catalog.tableExists("orders_silver"):
    daily_order_kpis = (
        spark.table("orders_silver")
        .withColumn("order_date", F.to_date("created_at"))
        .groupBy("order_date", "order_status")
        .agg(F.count("order_id").alias("order_count"), F.sum("amount").alias("total_amount"))
        .orderBy("order_date", "order_status")
    )
    daily_order_kpis.write.format("delta").mode("overwrite").saveAsTable("daily_order_kpis")
    gold_results["daily_order_kpis"] = daily_order_kpis.count()
    log("gold", "daily_order_kpis", daily_order_kpis.count(), "ok")

if spark.catalog.tableExists("logistics_silver"):
    sla_report = (
        spark.table("logistics_silver")
        .withColumn("on_time", F.col("actual_delivery_days") <= F.col("promised_delivery_days"))
        .groupBy("carrier")
        .agg(
            F.count("shipment_id").alias("shipment_count"),
            F.round(F.avg(F.col("on_time").cast("double")) * 100, 1).alias("on_time_rate"),
            F.round(F.avg("actual_delivery_days"), 2).alias("avg_actual_days"),
        )
    )
    sla_report.write.format("delta").mode("overwrite").saveAsTable("sla_report")
    gold_results["sla_report"] = sla_report.count()
    log("gold", "sla_report", sla_report.count(), "ok")

print(gold_results)

# COMMAND ----------

# MAGIC %md ## Data quality checks

# COMMAND ----------

warnings = []

if spark.catalog.tableExists("orders_silver"):
    orders = spark.table("orders_silver")
    if orders.groupBy("order_id").count().filter("count > 1").count() > 0:
        warnings.append("orders: duplicate order_id found after cleaning")
    if orders.filter(F.col("amount") < 0).count() > 0:
        warnings.append("orders: negative amount values found")

if spark.catalog.tableExists("logistics_silver"):
    logistics = spark.table("logistics_silver")
    if logistics.filter(F.col("actual_delivery_days") < 0).count() > 0:
        warnings.append("logistics: negative delivery days found")

if warnings:
    for w in warnings:
        log("data_quality", w, "-", "warning")
        print(f"WARNING: {w}")
else:
    log("data_quality", "all_checks", "-", "ok")
    print("No data quality issues found.")

# COMMAND ----------

import json

dbutils.notebook.exit(json.dumps({"gold": gold_results, "warnings": warnings}))
