"""
Basic tests for the data quality checks and CDC watermark logic.
Run with: pytest tests/
"""
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from data_quality import check_logistics, check_orders  # noqa: E402


def test_check_orders_flags_duplicate_ids():
    df = pd.DataFrame(
        {
            "order_id": ["ORD-1", "ORD-1"],
            "customer_id": ["CUST-1", "CUST-2"],
            "amount": [10.0, 20.0],
        }
    )
    warnings = check_orders(df)
    assert any("duplicate order_id" in w for w in warnings)


def test_check_orders_flags_negative_amount():
    df = pd.DataFrame(
        {
            "order_id": ["ORD-1"],
            "customer_id": ["CUST-1"],
            "amount": [-5.0],
        }
    )
    warnings = check_orders(df)
    assert any("negative amount" in w for w in warnings)


def test_check_orders_clean_data_has_no_warnings():
    df = pd.DataFrame(
        {
            "order_id": ["ORD-1", "ORD-2"],
            "customer_id": ["CUST-1", "CUST-2"],
            "amount": [10.0, 20.0],
        }
    )
    assert check_orders(df) == []


def test_check_logistics_flags_negative_delivery_days():
    df = pd.DataFrame(
        {
            "shipment_id": ["SHIP-1"],
            "actual_delivery_days": [-2],
        }
    )
    warnings = check_logistics(df)
    assert any("negative delivery days" in w for w in warnings)


@pytest.fixture
def temp_watermark(tmp_path, monkeypatch):
    from cdc_loader import save_watermark, get_watermark
    import config

    monkeypatch.setattr(config, "WATERMARK_STATE_PATH", tmp_path / "watermarks.json")
    import cdc_loader
    monkeypatch.setattr(cdc_loader, "WATERMARK_STATE_PATH", tmp_path / "watermarks.json")
    return save_watermark, get_watermark


def test_watermark_roundtrip(temp_watermark):
    save_watermark, get_watermark = temp_watermark
    save_watermark("orders", "2026-01-01T00:00:00")
    assert get_watermark("orders") == "2026-01-01T00:00:00"
