"""
SILVER layer: cleaned, typed, deduplicated, harmonized data. This is where
business rules for "what counts as valid data" get applied -- comparable to
the transformation (T) logic previously written as SQL / stored procedures.

# Databricks port note: same logic works almost unchanged on a Spark
# DataFrame; swap `df.dropna()` / `.astype()` for their PySpark equivalents.
"""
import pandas as pd

from config import BRONZE_DIR, SILVER_DIR, SOURCE_TABLES


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="order_id", keep="last").copy()
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["order_id", "customer_id", "amount"])
    df["created_at"] = pd.to_datetime(df["created_at"], format="ISO8601")
    df["updated_at"] = pd.to_datetime(df["updated_at"], format="ISO8601")
    df["order_status"] = df["order_status"].str.lower().str.strip()
    return df


def clean_logistics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset="shipment_id", keep="last").copy()
    df["promised_delivery_days"] = pd.to_numeric(
        df["promised_delivery_days"], errors="coerce"
    )
    df["actual_delivery_days"] = pd.to_numeric(
        df["actual_delivery_days"], errors="coerce"
    )
    df = df.dropna(subset=["shipment_id", "order_id"])
    df["created_at"] = pd.to_datetime(df["created_at"], format="ISO8601")
    df["updated_at"] = pd.to_datetime(df["updated_at"], format="ISO8601")
    df["carrier"] = df["carrier"].str.upper().str.strip()
    return df


CLEANERS = {"orders": clean_orders, "logistics": clean_logistics}


def run_silver() -> dict:
    results = {}
    for table_name in SOURCE_TABLES:
        bronze_path = BRONZE_DIR / f"{table_name}.csv"
        if not bronze_path.exists():
            results[table_name] = 0
            continue

        df = pd.read_csv(bronze_path)
        cleaned = CLEANERS[table_name](df)
        cleaned.to_csv(SILVER_DIR / f"{table_name}.csv", index=False)
        results[table_name] = len(cleaned)

    return results


if __name__ == "__main__":
    print(run_silver())
