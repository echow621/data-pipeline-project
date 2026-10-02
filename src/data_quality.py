"""
Lightweight data quality checks, run after the silver layer.
Mirrors the "only investigate when data looks abnormal" approach: cheap
checks run every time, and a warning is raised only when something is
actually off, rather than validating everything exhaustively each run.
"""
import pandas as pd

from config import SILVER_DIR


class DataQualityWarning(list):
    """Collects human-readable warning strings."""


def check_orders(df: pd.DataFrame) -> DataQualityWarning:
    warnings = DataQualityWarning()

    if df["order_id"].duplicated().any():
        warnings.append("orders: duplicate order_id found after cleaning")

    if (df["amount"] < 0).any():
        warnings.append("orders: negative amount values found")

    null_ratio = df["customer_id"].isna().mean()
    if null_ratio > 0.01:
        warnings.append(f"orders: {null_ratio:.1%} missing customer_id")

    return warnings


def check_logistics(df: pd.DataFrame) -> DataQualityWarning:
    warnings = DataQualityWarning()

    if df["shipment_id"].duplicated().any():
        warnings.append("logistics: duplicate shipment_id found after cleaning")

    if (df["actual_delivery_days"] < 0).any():
        warnings.append("logistics: negative delivery days found")

    return warnings


def run_data_quality() -> list:
    all_warnings = []

    orders_path = SILVER_DIR / "orders.csv"
    if orders_path.exists():
        all_warnings += check_orders(pd.read_csv(orders_path))

    logistics_path = SILVER_DIR / "logistics.csv"
    if logistics_path.exists():
        all_warnings += check_logistics(pd.read_csv(logistics_path))

    return all_warnings


if __name__ == "__main__":
    found = run_data_quality()
    if found:
        for w in found:
            print(f"WARNING: {w}")
    else:
        print("No data quality issues found.")
