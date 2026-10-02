"""
BRONZE layer: extracts raw rows from the source system, incrementally,
based on the CDC watermark. No transformation happens here -- bronze
should always mirror the source as closely as possible, so it can be
reprocessed if the silver/gold logic changes.

# Databricks port note: replace `pd.read_sql` with `spark.read.jdbc(...)`
# or a Delta/Auto Loader source, and write with
# `df.write.format("delta").mode("append").save(bronze_path)`.
# CSV is used here instead of Parquet only to keep this project
# dependency-free (no pyarrow needed) -- swap `to_csv`/`read_csv` for
# `to_parquet`/`read_parquet` for a production-realistic setup.
"""
import sqlite3

import pandas as pd

from cdc_loader import get_watermark, save_watermark
from config import BRONZE_DIR, RAW_DB_PATH, SOURCE_TABLES


def extract_table(conn, table_name: str) -> pd.DataFrame:
    watermark = get_watermark(table_name)
    if watermark:
        query = f"SELECT * FROM {table_name} WHERE updated_at > ?"
        df = pd.read_sql(query, conn, params=(watermark,))
    else:
        # First run: full load
        df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    return df


def run_bronze() -> dict:
    """Returns a dict of {table_name: row_count_loaded} for logging."""
    conn = sqlite3.connect(RAW_DB_PATH)
    results = {}

    for table_name in SOURCE_TABLES:
        df = extract_table(conn, table_name)
        results[table_name] = len(df)

        if not df.empty:
            bronze_path = BRONZE_DIR / f"{table_name}.csv"
            key_col = "order_id" if table_name == "orders" else "shipment_id"

            if bronze_path.exists():
                existing = pd.read_csv(bronze_path)
                combined = pd.concat([existing, df]).drop_duplicates(
                    subset=key_col, keep="last"
                )
                combined.to_csv(bronze_path, index=False)
            else:
                df.to_csv(bronze_path, index=False)

            new_watermark = df["updated_at"].max()
            save_watermark(table_name, new_watermark)

    conn.close()
    return results


if __name__ == "__main__":
    print(run_bronze())
