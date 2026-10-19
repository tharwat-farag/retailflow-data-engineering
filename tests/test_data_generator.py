"""Automated tests for RetailFlow synthetic data generator."""

import csv
from datetime import datetime
from pathlib import Path
import pytest

from src.data_generator.generate_data import RetailDataGenerator


@pytest.fixture(scope="module")
def generated_dataset(tmp_path_factory):
    """Generates a small test dataset in an isolated temporary directory."""
    temp_dir = tmp_path_factory.mktemp("retailflow_test_data")
    raw_dir = temp_dir / "raw"
    sample_dir = temp_dir / "sample"

    generator = RetailDataGenerator(
        seed=42,
        output_dir=str(raw_dir),
        sample_dir=str(sample_dir),
    )
    summary = generator.run_all(
        num_suppliers=15,
        num_products=50,
        num_orders=150,
    )

    return {
        "raw_dir": raw_dir,
        "sample_dir": sample_dir,
        "summary": summary,
        "generator": generator,
    }


def read_csv_rows(file_path: Path):
    """Helper to read CSV into list of dicts."""
    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def test_files_creation(generated_dataset):
    """Verifies that all 6 required CSV files are created in both raw and sample directories."""
    expected_files = [
        "suppliers.csv",
        "warehouses.csv",
        "products.csv",
        "inventory.csv",
        "purchase_orders.csv",
        "purchase_order_items.csv",
    ]

    for filename in expected_files:
        raw_file = generated_dataset["raw_dir"] / filename
        sample_file = generated_dataset["sample_dir"] / filename

        assert raw_file.exists(), f"Raw file missing: {filename}"
        assert raw_file.stat().st_size > 0, f"Raw file is empty: {filename}"

        assert sample_file.exists(), f"Sample file missing: {filename}"
        sample_rows = read_csv_rows(sample_file)
        assert len(sample_rows) <= 5, f"Sample file exceeded 5 rows: {filename}"


def test_primary_key_uniqueness(generated_dataset):
    """Verifies that primary key columns have 100% unique values."""
    pk_checks = [
        ("suppliers.csv", "supplier_id"),
        ("warehouses.csv", "warehouse_id"),
        ("products.csv", "product_id"),
        ("inventory.csv", "inventory_id"),
        ("purchase_orders.csv", "order_id"),
        ("purchase_order_items.csv", "order_item_id"),
    ]

    for filename, pk_col in pk_checks:
        rows = read_csv_rows(generated_dataset["raw_dir"] / filename)
        pk_values = [r[pk_col] for r in rows]
        assert len(pk_values) == len(set(pk_values)), f"Duplicate primary keys found in {filename} ({pk_col})"


def test_referential_integrity(generated_dataset):
    """Ensures foreign key references are valid across all entities (no orphans)."""
    suppliers = {r["supplier_id"] for r in read_csv_rows(generated_dataset["raw_dir"] / "suppliers.csv")}
    warehouses = {r["warehouse_id"] for r in read_csv_rows(generated_dataset["raw_dir"] / "warehouses.csv")}
    products = {r["product_id"] for r in read_csv_rows(generated_dataset["raw_dir"] / "products.csv")}
    orders = {r["order_id"] for r in read_csv_rows(generated_dataset["raw_dir"] / "purchase_orders.csv")}

    # 1. Inventory FKs
    for r in read_csv_rows(generated_dataset["raw_dir"] / "inventory.csv"):
        assert r["warehouse_id"] in warehouses, f"Inventory references invalid warehouse: {r['warehouse_id']}"
        assert r["product_id"] in products, f"Inventory references invalid product: {r['product_id']}"

    # 2. Purchase Orders FKs
    for r in read_csv_rows(generated_dataset["raw_dir"] / "purchase_orders.csv"):
        assert r["supplier_id"] in suppliers, f"PO references invalid supplier: {r['supplier_id']}"
        assert r["warehouse_id"] in warehouses, f"PO references invalid warehouse: {r['warehouse_id']}"

    # 3. Purchase Order Items FKs
    for r in read_csv_rows(generated_dataset["raw_dir"] / "purchase_order_items.csv"):
        assert r["order_id"] in orders, f"PO item references invalid order: {r['order_id']}"
        assert r["product_id"] in products, f"PO item references invalid product: {r['product_id']}"


def test_product_pricing_and_stock_targets(generated_dataset):
    """Validates realistic price margins and reorder limits."""
    products = read_csv_rows(generated_dataset["raw_dir"] / "products.csv")

    for prod in products:
        cost = float(prod["unit_cost"])
        price = float(prod["unit_price"])
        reorder_pt = int(prod["reorder_point"])
        target_stock = int(prod["target_stock_level"])

        assert cost > 0, f"Unit cost must be positive: {prod['product_id']}"
        assert price > cost, f"Unit price must be greater than unit cost: {prod['product_id']}"
        assert reorder_pt > 0, f"Reorder point must be positive: {prod['product_id']}"
        assert target_stock > reorder_pt, f"Target stock must be greater than reorder point: {prod['product_id']}"


def test_inventory_scenarios_and_stockouts(generated_dataset):
    """Verifies that stockout cases (stock_on_hand == 0) and low-stock cases exist for downstream analytics."""
    inventory = read_csv_rows(generated_dataset["raw_dir"] / "inventory.csv")

    stockouts = [r for r in inventory if int(r["stock_on_hand"]) == 0]
    in_stock = [r for r in inventory if int(r["stock_on_hand"]) > 0]

    assert len(stockouts) > 0, "No stockout cases generated in inventory (needed for analytics queries)"
    assert len(in_stock) > 0, "No in-stock cases generated"

    for r in inventory:
        soh = int(r["stock_on_hand"])
        allocated = int(r["stock_allocated"])
        reserved = int(r["stock_reserved"])
        assert soh >= 0
        assert allocated >= 0
        assert reserved >= 0
        assert (allocated + reserved) <= soh or soh == 0


def test_purchase_order_dates_and_delays(generated_dataset):
    """Verifies order timeline logic and presence of delayed deliveries."""
    orders = read_csv_rows(generated_dataset["raw_dir"] / "purchase_orders.csv")

    statuses = {r["order_status"] for r in orders}
    assert "RECEIVED" in statuses
    assert "PENDING" in statuses

    delayed_orders = []
    for r in orders:
        order_date = datetime.fromisoformat(r["order_date"])
        expected_date = datetime.fromisoformat(r["expected_delivery_date"])
        assert expected_date >= order_date, f"Expected delivery before order date: {r['order_id']}"

        if r["actual_delivery_date"]:
            actual_date = datetime.fromisoformat(r["actual_delivery_date"])
            assert actual_date >= order_date, f"Actual delivery before order date: {r['order_id']}"
            if actual_date > expected_date:
                delayed_orders.append(r)

    assert len(delayed_orders) > 0, "No delayed deliveries generated (needed for supplier performance analytics)"


def test_date_formats_and_temporal_validity(generated_dataset):
    """Verifies that all date columns strictly follow ISO 8601 (YYYY-MM-DD) and logical timelines."""
    today_str = datetime.now().date().isoformat()

    # 1. Inventory last_counted_date
    for r in read_csv_rows(generated_dataset["raw_dir"] / "inventory.csv"):
        count_date = r["last_counted_date"]
        dt = datetime.strptime(count_date, "%Y-%m-%d").date()
        assert dt.isoformat() <= today_str, f"Inventory count date in the future: {count_date}"

    # 2. Purchase Orders dates
    for r in read_csv_rows(generated_dataset["raw_dir"] / "purchase_orders.csv"):
        order_date_str = r["order_date"]
        expected_str = r["expected_delivery_date"]
        actual_str = r["actual_delivery_date"]
        status = r["order_status"]

        order_dt = datetime.strptime(order_date_str, "%Y-%m-%d").date()
        expected_dt = datetime.strptime(expected_str, "%Y-%m-%d").date()

        assert order_dt.isoformat() <= today_str, f"Order date in future: {order_date_str}"
        assert expected_dt >= order_dt, f"Expected delivery before order: {r['order_id']}"

        if status in ("RECEIVED", "PARTIALLY_RECEIVED"):
            assert actual_str != "", f"Order {r['order_id']} is {status} but actual_delivery_date is missing"
            actual_dt = datetime.strptime(actual_str, "%Y-%m-%d").date()
            assert actual_dt >= order_dt, f"Actual delivery before order: {r['order_id']}"
            assert actual_dt.isoformat() <= today_str, f"Received order has actual delivery in future: {actual_str}"
        elif status in ("PENDING", "CANCELLED"):
            assert actual_str == "", f"Order {r['order_id']} is {status} but has actual_delivery_date: {actual_str}"


def test_purchase_order_items_and_totals(generated_dataset):
    """Verifies PO item quantities, pricing math, and parent total_amount consistency."""
    orders = read_csv_rows(generated_dataset["raw_dir"] / "purchase_orders.csv")
    order_items = read_csv_rows(generated_dataset["raw_dir"] / "purchase_order_items.csv")

    order_status_lookup = {r["order_id"]: r["order_status"] for r in orders}
    order_total_lookup = {r["order_id"]: float(r["total_amount"]) for r in orders}
    items_by_order = {}

    for item in order_items:
        oid = item["order_id"]
        items_by_order.setdefault(oid, []).append(item)

        qty_ordered = int(item["quantity_ordered"])
        qty_received = int(item["quantity_received"])
        unit_price = float(item["unit_price"])
        line_total = float(item["line_total"])

        assert qty_ordered > 0, f"Quantity ordered must be positive: {item['order_item_id']}"
        assert unit_price > 0, f"Unit price must be positive: {item['order_item_id']}"
        assert round(qty_ordered * unit_price, 2) == round(line_total, 2), f"Line total mismatch in item: {item['order_item_id']}"
        assert qty_received <= qty_ordered, f"Received quantity exceeds ordered in {item['order_item_id']}"

        status = order_status_lookup[oid]
        if status == "RECEIVED":
            assert qty_received == qty_ordered, f"Order is RECEIVED but item received != ordered: {item['order_item_id']}"
        elif status == "PARTIALLY_RECEIVED":
            assert 0 < qty_received < qty_ordered, f"Order is PARTIALLY_RECEIVED but invalid qty_received: {item['order_item_id']}"
        elif status in ("PENDING", "CANCELLED"):
            assert qty_received == 0, f"Order is {status} but item has qty_received > 0: {item['order_item_id']}"

    # Verify every order has items and totals match
    for oid, expected_total in order_total_lookup.items():
        assert oid in items_by_order, f"Order {oid} has no order items"
        computed_total = sum(float(item["line_total"]) for item in items_by_order[oid])
        assert abs(computed_total - expected_total) < 0.02, f"Total amount mismatch for {oid}: {expected_total} vs {computed_total}"


def test_composite_keys_uniqueness(generated_dataset):
    """Verifies uniqueness of composite keys (warehouse+product in inventory, order+product in PO items)."""
    # 1. Warehouse + Product pair in inventory
    inv_rows = read_csv_rows(generated_dataset["raw_dir"] / "inventory.csv")
    wh_prod_pairs = [(r["warehouse_id"], r["product_id"]) for r in inv_rows]
    assert len(wh_prod_pairs) == len(set(wh_prod_pairs)), "Duplicate warehouse-product combination in inventory"

    # 2. Order + Product pair in purchase order items
    poi_rows = read_csv_rows(generated_dataset["raw_dir"] / "purchase_order_items.csv")
    order_prod_pairs = [(r["order_id"], r["product_id"]) for r in poi_rows]
    assert len(order_prod_pairs) == len(set(order_prod_pairs)), "Duplicate order-product combination in PO items"


def test_suppliers_and_warehouses_data_consistency(generated_dataset):
    """Verifies value ranges and constraints for suppliers and warehouses."""
    suppliers = read_csv_rows(generated_dataset["raw_dir"] / "suppliers.csv")
    for s in suppliers:
        rel = float(s["reliability_score"])
        lead_time = int(s["lead_time_days"])
        email = s["contact_email"]
        assert 0.0 < rel <= 1.0, f"Invalid reliability score: {rel}"
        assert lead_time > 0, f"Invalid lead time: {lead_time}"
        assert "@" in email and email.endswith(".com"), f"Invalid email format: {email}"

    warehouses = read_csv_rows(generated_dataset["raw_dir"] / "warehouses.csv")
    for w in warehouses:
        cap = int(w["capacity_units"])
        assert cap > 0, f"Warehouse capacity must be positive: {w['warehouse_id']}"
        assert w["warehouse_id"].startswith("WH")
        assert len(w["warehouse_name"]) > 0


def test_output_path_resolution_and_safeguards(tmp_path):
    """Ensures generator saves to project root data/raw by default and forbids paths within src/."""
    from src.data_generator.generate_data import PROJECT_ROOT

    # Default paths should resolve to PROJECT_ROOT / "data" / "raw"
    gen = RetailDataGenerator(seed=1)
    assert gen.output_dir == PROJECT_ROOT / "data" / "raw"
    assert gen.sample_dir == PROJECT_ROOT / "data" / "sample"
    assert not gen.output_dir.resolve().is_relative_to(PROJECT_ROOT / "src")

    # Explicit relative path should also resolve relative to PROJECT_ROOT
    gen_rel = RetailDataGenerator(seed=1, output_dir="data/raw", sample_dir="data/sample")
    assert gen_rel.output_dir == PROJECT_ROOT / "data" / "raw"
    assert gen_rel.sample_dir == PROJECT_ROOT / "data" / "sample"

    # Trying to place output inside src/ must raise ValueError
    src_target = PROJECT_ROOT / "src" / "data" / "raw"
    with pytest.raises(ValueError, match="Output directory cannot be located inside src/"):
        RetailDataGenerator(output_dir=src_target)
