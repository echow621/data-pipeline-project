"""
GOLD layer: business-ready aggregates, comparable to the KPI / SLA reports
previously built in Jasper. Two KPI tables are produced:

- daily_order_kpis: order volume and revenue per day and status
- sla_report: on-time delivery rate per carrier (SLA-style metric)

# Databricks port note: these aggregations map directly to a Spark
# groupBy/agg, and could be exposed to BI tools as a Gold Delta table.
"""
import pandas as pd

from config import GOLD_DIR, SILVER_DIR


def build_daily_order_kpis() -> pd.DataFrame:
    orders = pd.read_csv(SILVER_DIR / "orders.csv", parse_dates=["created_at"])
    orders["order_date"] = orders["created_at"].dt.date

    kpis = (
        orders.groupby(["order_date", "order_status"])
        .agg(order_count=("order_id", "count"), total_amount=("amount", "sum"))
        .reset_index()
        .sort_values(["order_date", "order_status"])
    )
    return kpis


def build_sla_report() -> pd.DataFrame:
    logistics = pd.read_csv(SILVER_DIR / "logistics.csv")
    logistics["on_time"] = (
        logistics["actual_delivery_days"] <= logistics["promised_delivery_days"]
    )

    sla = (
        logistics.groupby("carrier")
        .agg(
            shipment_count=("shipment_id", "count"),
            on_time_rate=("on_time", "mean"),
            avg_actual_days=("actual_delivery_days", "mean"),
        )
        .reset_index()
    )
    sla["on_time_rate"] = (sla["on_time_rate"] * 100).round(1)
    sla["avg_actual_days"] = sla["avg_actual_days"].round(2)
    return sla


def run_gold() -> dict:
    results = {}

    if (SILVER_DIR / "orders.csv").exists():
        kpis = build_daily_order_kpis()
        kpis.to_csv(GOLD_DIR / "daily_order_kpis.csv", index=False)
        results["daily_order_kpis"] = len(kpis)

    if (SILVER_DIR / "logistics.csv").exists():
        sla = build_sla_report()
        sla.to_csv(GOLD_DIR / "sla_report.csv", index=False)
        results["sla_report"] = len(sla)

    return results


if __name__ == "__main__":
    print(run_gold())
