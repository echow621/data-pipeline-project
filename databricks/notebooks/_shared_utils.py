# Databricks notebook source
# MAGIC %md
# MAGIC # Shared utilities
# MAGIC
# MAGIC Not run directly -- included via `%run ./_shared_utils` at the top of
# MAGIC `01_bronze_layer.py`, `02_silver_layer.py` and `03_gold_layer.py`, so the
# MAGIC watermark and logging functions exist in exactly one place instead of
# MAGIC being copy-pasted into every notebook.
# MAGIC
# MAGIC `%run` executes this notebook's cells inside the *calling* notebook's
# MAGIC session -- all functions and variables defined here become directly
# MAGIC usable there afterwards, as if they'd been defined in that notebook.
# MAGIC This is the standard Databricks pattern for sharing code between
# MAGIC notebooks without packaging a separate library.
# MAGIC
# MAGIC Assumes the calling notebook has already run `spark.sql(f"USE
# MAGIC {catalog}.{schema}")`, so `pipeline_log` / `cdc_watermarks` are created
# MAGIC in the right place.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS pipeline_log (
        run_timestamp TIMESTAMP,
        stage STRING,
        table_name STRING,
        row_count STRING,
        status STRING
    ) USING DELTA
    """
)


def log(stage, table_name, row_count, status):
    spark.sql(
        f"""
        INSERT INTO pipeline_log VALUES
        (current_timestamp(), '{stage}', '{table_name}', '{row_count}', '{status}')
        """
    )

# COMMAND ----------

def get_watermark(table_name: str):
    row = (
        spark.table("cdc_watermarks")
        .filter(F.col("table_name") == table_name)
        .orderBy(F.col("updated_at").desc())
        .limit(1)
        .collect()
    )
    return row[0]["watermark_value"] if row else None


def save_watermark(table_name: str, watermark_value: str):
    spark.sql(
        f"""
        MERGE INTO cdc_watermarks AS target
        USING (SELECT '{table_name}' AS table_name,
                      '{watermark_value}' AS watermark_value,
                      current_timestamp() AS updated_at) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
