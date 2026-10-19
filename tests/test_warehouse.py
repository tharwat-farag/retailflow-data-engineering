"""Automated test suite for PostgreSQL Data Warehouse and Loader."""

from datetime import date, datetime
import os
from pathlib import Path
import pytest
from sqlalchemy import create_engine, text

from src.warehouse.loader import (
    WarehouseConfig,
    WarehouseConnectionError,
    WarehouseLoader,
    WarehouseLoadError,
    WarehouseTransactionError,
    main,
)


def test_warehouse_config_defaults():
    """Verifies that default connection parameters construct a valid connection URL."""
    config = WarehouseConfig()
    url = config.connection_url

    assert "postgresql+psycopg2://" in url
    assert "retailflow_admin" in url
    assert "retailflow_warehouse" in url
    assert "localhost:5432" in url


def test_warehouse_config_custom_overrides(monkeypatch):
    """Verifies environment variable overrides for database configuration."""
    monkeypatch.setenv("POSTGRES_USER", "custom_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "custom_pass")
    monkeypatch.setenv("POSTGRES_HOST", "db.example.internal")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "custom_warehouse")

    config = WarehouseConfig()
    url = config.connection_url

    assert "custom_user:custom_pass@db.example.internal:5433/custom_warehouse" in url


def test_schema_sql_ddl_structure():
    """Verifies that 01_init_warehouse.sql and 02_indexes.sql contain all required tables."""
    init_sql = Path("sql/schema/01_init_warehouse.sql")
    indexes_sql = Path("sql/schema/02_indexes.sql")

    assert init_sql.exists(), "Missing 01_init_warehouse.sql"
    assert indexes_sql.exists(), "Missing 02_indexes.sql"

    content = init_sql.read_text(encoding="utf-8")

    # Verify staging tables
    assert "CREATE TABLE IF NOT EXISTS staging.stg_suppliers" in content
    assert "CREATE TABLE IF NOT EXISTS staging.stg_warehouses" in content
    assert "CREATE TABLE IF NOT EXISTS staging.stg_products" in content
    assert "CREATE TABLE IF NOT EXISTS staging.stg_inventory" in content
    assert "CREATE TABLE IF NOT EXISTS staging.stg_purchase_orders" in content
    assert "CREATE TABLE IF NOT EXISTS staging.stg_purchase_order_items" in content

    # Verify dimensional star schema tables
    assert "CREATE TABLE IF NOT EXISTS analytics.dim_date" in content
    assert "CREATE TABLE IF NOT EXISTS analytics.dim_supplier" in content
    assert "CREATE TABLE IF NOT EXISTS analytics.dim_warehouse" in content
    assert "CREATE TABLE IF NOT EXISTS analytics.dim_product" in content
    assert "CREATE TABLE IF NOT EXISTS analytics.fact_inventory_snapshot" in content
    assert "CREATE TABLE IF NOT EXISTS analytics.fact_purchase_order_fulfillment" in content

    # Verify indexes
    idx_content = indexes_sql.read_text(encoding="utf-8")
    assert "idx_fact_inv_date_key" in idx_content
    assert "idx_fact_po_order_date" in idx_content
    assert "idx_fact_po_sup_key" in idx_content


def test_date_dimension_record_generation():
    """Verifies calendar date dimension generation, date keys, and calendar metadata."""
    sample_dates = {"2026-10-18", "2026-10-17", "2026-01-01", ""}
    records = WarehouseLoader.generate_date_dimension_records(sample_dates)

    assert len(records) == 3  # empty string excluded

    rec_sunday = next(r for r in records if r["date_key"] == 20261018)
    assert rec_sunday["full_date"] == date(2026, 10, 18)
    assert rec_sunday["year"] == 2026
    assert rec_sunday["quarter"] == 4
    assert rec_sunday["month"] == 10
    assert rec_sunday["month_name"] == "October"
    assert rec_sunday["day"] == 18
    assert rec_sunday["day_of_week"] == 7  # Sunday
    assert rec_sunday["day_name"] == "Sunday"
    assert rec_sunday["is_weekend"] is True

    rec_saturday = next(r for r in records if r["date_key"] == 20261017)
    assert rec_saturday["is_weekend"] is True

    rec_new_year = next(r for r in records if r["date_key"] == 20260101)
    assert rec_new_year["quarter"] == 1
    assert rec_new_year["day_name"] == "Thursday"
    assert rec_new_year["is_weekend"] is False


def test_connection_failure_handling():
    """Verifies that an unreachable database port raises WarehouseConnectionError."""
    # Point to an unassigned port
    bad_config = WarehouseConfig(host="127.0.0.1", port=59999)
    loader = WarehouseLoader(config=bad_config)

    with pytest.raises(WarehouseConnectionError) as exc_info:
        loader.test_connection()

    assert "Failed to connect to database" in str(exc_info.value)


def test_analytical_sql_queries_integrity():
    """Verifies that supply_chain_analytics.sql contains all five analytical queries."""
    sql_file = Path("sql/analytics/supply_chain_analytics.sql")
    assert sql_file.exists(), "Missing supply_chain_analytics.sql"

    content = sql_file.read_text(encoding="utf-8")
    assert "INVENTORY STOCKOUT & REORDER RISK" in content
    assert "SUPPLIER DELIVERY PERFORMANCE" in content
    assert "DELAYED PURCHASE ORDERS" in content
    assert "WAREHOUSE STORAGE & UTILIZATION" in content
    assert "PRODUCT CATEGORY MARGIN" in content


def test_sqlite_mock_relational_loading_and_idempotency(tmp_path):
    """Simulates relational loading and idempotency using an in-memory/temporary SQLite database."""
    # Create SQLite engine
    db_file = tmp_path / "test_warehouse.db"
    sqlite_url = f"sqlite:///{db_file}"
    engine = create_engine(sqlite_url)

    # Initialize SQLite schema
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE dim_supplier (
                supplier_key INTEGER PRIMARY KEY AUTOINCREMENT,
                supplier_id TEXT NOT NULL UNIQUE,
                supplier_name TEXT NOT NULL,
                reliability_tier TEXT NOT NULL
            );
        """))
        conn.execute(text("""
            CREATE TABLE dim_product (
                product_key INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL UNIQUE,
                product_name TEXT NOT NULL,
                unit_price REAL NOT NULL
            );
        """))
        conn.execute(text("""
            CREATE TABLE fact_purchase_order_fulfillment (
                fulfillment_key INTEGER PRIMARY KEY AUTOINCREMENT,
                order_item_id TEXT NOT NULL UNIQUE,
                supplier_key INTEGER NOT NULL REFERENCES dim_supplier(supplier_key),
                product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
                line_total REAL NOT NULL
            );
        """))

    # Test load 1
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dim_supplier (supplier_id, supplier_name, reliability_tier)
            VALUES ('SUP001', 'Apex Logistics', 'Tier 1 - Excellent')
            ON CONFLICT (supplier_id) DO UPDATE SET supplier_name = EXCLUDED.supplier_name;
        """))
        conn.execute(text("""
            INSERT INTO dim_product (product_id, product_name, unit_price)
            VALUES ('PRD0001', 'Yoga Mat Pro', 69.43)
            ON CONFLICT (product_id) DO UPDATE SET unit_price = EXCLUDED.unit_price;
        """))
        conn.execute(text("""
            INSERT INTO fact_purchase_order_fulfillment (order_item_id, supplier_key, product_key, line_total)
            VALUES ('POI000001', 1, 1, 550.0)
            ON CONFLICT (order_item_id) DO UPDATE SET line_total = EXCLUDED.line_total;
        """))

    # Verify counts after load 1
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM dim_supplier")).scalar() == 1
        assert conn.execute(text("SELECT COUNT(*) FROM dim_product")).scalar() == 1
        assert conn.execute(text("SELECT COUNT(*) FROM fact_purchase_order_fulfillment")).scalar() == 1

    # Test load 2 (Rerun Idempotency)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO dim_supplier (supplier_id, supplier_name, reliability_tier)
            VALUES ('SUP001', 'Apex Logistics Updated', 'Tier 1 - Excellent')
            ON CONFLICT (supplier_id) DO UPDATE SET supplier_name = EXCLUDED.supplier_name;
        """))
        conn.execute(text("""
            INSERT INTO fact_purchase_order_fulfillment (order_item_id, supplier_key, product_key, line_total)
            VALUES ('POI000001', 1, 1, 550.0)
            ON CONFLICT (order_item_id) DO UPDATE SET line_total = EXCLUDED.line_total;
        """))

    # Counts must be identical (no duplicates created)
    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM dim_supplier")).scalar() == 1
        assert conn.execute(text("SELECT COUNT(*) FROM fact_purchase_order_fulfillment")).scalar() == 1
        sup_name = conn.execute(text("SELECT supplier_name FROM dim_supplier WHERE supplier_id='SUP001'")).scalar()
        assert sup_name == "Apex Logistics Updated"

    engine.dispose()


def test_transaction_rollback_on_failure(tmp_path):
    """Verifies that an error in a database transaction executes an automatic rollback."""
    db_file = tmp_path / "rollback_test.db"
    engine = create_engine(f"sqlite:///{db_file}")

    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT);"))

    # Attempt transaction that inserts 1 row and then fails on second
    with pytest.raises(Exception):
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO test_table VALUES (1, 'Initial');"))
            # Trigger SQL error (duplicate PK)
            conn.execute(text("INSERT INTO test_table VALUES (1, 'Duplicate');"))

    # Verify rollback: table should be empty
    with engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
        assert count == 0, "Transaction did not rollback properly on failure!"

    engine.dispose()



def test_cli_main_connection_handling(monkeypatch):
    """Verifies that CLI main gracefully exits with error 1 when PostgreSQL is offline."""
    # Point to unreachable port
    monkeypatch.setenv("POSTGRES_PORT", "59999")
    test_args = ["loader.py"]
    monkeypatch.setattr("sys.argv", test_args)

    with pytest.raises(SystemExit) as exc_info:
        main()

    assert exc_info.value.code == 1
