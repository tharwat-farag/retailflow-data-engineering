"""Automated test suite for RetailFlow Silver Layer Transformations."""

import csv
import json
from pathlib import Path
import pytest

from src.data_generator.generate_data import RetailDataGenerator
from src.ingestion.ingest_bronze import BronzeDataIngestion
from src.transformation.transform_silver import (
    EXPECTED_DATASETS,
    SilverDataTransformer,
    ValidationHaltError,
    main,
)


@pytest.fixture
def pipeline_dirs(tmp_path):
    """Sets up raw, bronze, and silver directories with generated and ingested data."""
    raw_dir = tmp_path / "raw"
    sample_dir = tmp_path / "sample"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    # Phase 1: Generate synthetic raw data
    gen = RetailDataGenerator(seed=42, output_dir=str(raw_dir), sample_dir=str(sample_dir))
    gen.run_all(num_suppliers=5, num_products=15, num_orders=25)

    # Phase 2: Ingest into Bronze
    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)
    ingestion.ingest_all()

    return {
        "raw_dir": raw_dir,
        "bronze_dir": bronze_dir,
        "silver_dir": silver_dir,
    }


def read_csv_rows(path: Path):
    """Helper to read rows as list of dicts."""
    with open(path, mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv_rows(path: Path, rows: list, fieldnames: list):
    """Helper to write rows from dicts."""
    with open(path, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_successful_silver_transformation(pipeline_dirs):
    """Verifies that all 6 datasets are transformed to Silver with enriched attributes."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    summary = transformer.transform_all(validate_first=True)

    assert summary["status"] == "SUCCESS"
    assert summary["total_input_records"] > 0
    assert summary["total_output_records"] == summary["total_input_records"]
    assert summary["total_quarantined"] == 0

    # Verify all 6 silver CSV files exist
    for filename in EXPECTED_DATASETS:
        silver_file = silver_dir / filename
        assert silver_file.exists(), f"Missing silver file: {filename}"
        assert silver_file.stat().st_size > 0

    # Verify transformation report
    report_file = silver_dir / "transformation_report.json"
    assert report_file.exists()
    with open(report_file, encoding="utf-8") as f:
        report = json.load(f)
    assert report["overall_status"] == "SUCCESS"
    assert report["summary"]["total_output_records"] == summary["total_output_records"]


def test_text_whitespace_and_casing_standardization(pipeline_dirs):
    """Verifies text field whitespace collapse, state code uppercasing, and email lowercasing."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    # Inject dirty whitespace into Bronze suppliers
    sup_file = bronze_dir / "suppliers.csv"
    rows = read_csv_rows(sup_file)
    rows[0]["supplier_name"] = "   Acme    Logistics   Ltd   "
    rows[0]["contact_email"] = "  SUPPLY@ACME.COM  "
    write_csv_rows(sup_file, rows, list(rows[0].keys()))

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    transformer.transform_all(validate_first=False)

    silver_rows = read_csv_rows(silver_dir / "suppliers.csv")
    assert silver_rows[0]["supplier_name"] == "Acme Logistics Ltd"
    assert silver_rows[0]["contact_email"] == "supply@acme.com"


def test_numeric_rounding_and_date_standardization(pipeline_dirs):
    """Verifies strict decimal precision formatting and ISO date standardization."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    transformer.transform_all(validate_first=False)

    # Check products currency decimals
    prd_rows = read_csv_rows(silver_dir / "products.csv")
    for r in prd_rows:
        cost_parts = r["unit_cost"].split(".")
        price_parts = r["unit_price"].split(".")
        margin_parts = r["margin_amount"].split(".")
        assert len(cost_parts) == 2 and len(cost_parts[1]) == 2
        assert len(price_parts) == 2 and len(price_parts[1]) == 2
        assert len(margin_parts) == 2 and len(margin_parts[1]) == 2

    # Check date formatting in purchase_orders
    po_rows = read_csv_rows(silver_dir / "purchase_orders.csv")
    for r in po_rows:
        assert len(r["order_date"]) == 10
        assert len(r["expected_delivery_date"]) == 10
        if r["actual_delivery_date"]:
            assert len(r["actual_delivery_date"]) == 10


def test_derived_metrics_and_business_rules(pipeline_dirs):
    """Verifies derived fields: product margin, inventory availability, delay days, and fulfillment ratios."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    transformer.transform_all(validate_first=False)

    # 1. Product Margins
    prd_rows = read_csv_rows(silver_dir / "products.csv")
    for r in prd_rows:
        cost = float(r["unit_cost"])
        price = float(r["unit_price"])
        margin_amt = float(r["margin_amount"])
        margin_pct = float(r["margin_percentage"])
        assert round(price - cost, 2) == round(margin_amt, 2)
        assert round((margin_amt / price) * 100, 2) == round(margin_pct, 2)

    # 2. Inventory Availability & Status
    inv_rows = read_csv_rows(silver_dir / "inventory.csv")
    for r in inv_rows:
        soh = int(r["stock_on_hand"])
        alloc = int(r["stock_allocated"])
        res = int(r["stock_reserved"])
        avail = int(r["stock_available"])
        status = r["inventory_status"]

        assert avail == max(0, soh - alloc - res)
        assert status in ("OUT_OF_STOCK", "LOW_STOCK", "HEALTHY", "OVERSTOCK")
        if soh == 0:
            assert status == "OUT_OF_STOCK"

    # 3. Purchase Orders Delay Tracking
    po_rows = read_csv_rows(silver_dir / "purchase_orders.csv")
    for r in po_rows:
        ord_status = r["order_status"]
        if ord_status in ("PENDING", "CANCELLED"):
            assert r["actual_delivery_date"] == ""
            assert r["delivery_delay_days"] == ""
            assert r["is_delayed"] == ""
        else:
            assert r["actual_delivery_date"] != ""
            assert r["is_delayed"] in ("TRUE", "FALSE")


def test_exact_duplicates_deduplicated(pipeline_dirs):
    """Verifies exact duplicate rows are safely deduplicated and counted."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    # Duplicate a row in suppliers.csv
    sup_file = bronze_dir / "suppliers.csv"
    rows = read_csv_rows(sup_file)
    original_count = len(rows)
    rows.append(rows[0].copy())
    write_csv_rows(sup_file, rows, list(rows[0].keys()))

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    summary = transformer.transform_all(validate_first=False)

    silver_rows = read_csv_rows(silver_dir / "suppliers.csv")
    assert len(silver_rows) == original_count
    assert summary["total_deduplicated"] == 1


def test_conflicting_duplicates_quarantined(pipeline_dirs):
    """Verifies conflicting primary keys are quarantined instead of silently discarded."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    # Ingest a conflicting record with same ID but different name
    sup_file = bronze_dir / "suppliers.csv"
    rows = read_csv_rows(sup_file)
    conflict = rows[0].copy()
    conflict["supplier_name"] = "Conflicting Impostor Corp"
    rows.append(conflict)
    write_csv_rows(sup_file, rows, list(rows[0].keys()))

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    summary = transformer.transform_all(validate_first=False)

    assert summary["total_quarantined"] == 1
    quarantine_file = silver_dir / "quarantine" / "suppliers_quarantine.json"
    assert quarantine_file.exists()

    with open(quarantine_file, encoding="utf-8") as f:
        q_data = json.load(f)
    assert q_data["count"] == 1
    assert q_data["records"][0]["reason"] == "CONFLICTING_PRIMARY_KEY"


def test_foreign_key_orphan_quarantine(pipeline_dirs):
    """Verifies records referencing non-existent parent keys are quarantined."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    # Ingest an inventory record with invalid warehouse_id
    inv_file = bronze_dir / "inventory.csv"
    rows = read_csv_rows(inv_file)
    rows[0]["warehouse_id"] = "WH_DOES_NOT_EXIST"
    write_csv_rows(inv_file, rows, list(rows[0].keys()))

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    summary = transformer.transform_all(validate_first=False)

    assert summary["total_quarantined"] == 1
    q_file = silver_dir / "quarantine" / "inventory_quarantine.json"
    assert q_file.exists()

    with open(q_file, encoding="utf-8") as f:
        q_data = json.load(f)
    assert q_data["records"][0]["reason"] == "FOREIGN_KEY_VIOLATION"


def test_validation_failure_halts_silver_transformation(pipeline_dirs):
    """Verifies that upstream validation failure halts Silver transformation and raises ValidationHaltError."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    # Corrupt a bronze file (truncate to 0 bytes) to cause validation failure
    (bronze_dir / "products.csv").write_text("", encoding="utf-8")

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)

    with pytest.raises(ValidationHaltError) as exc_info:
        transformer.transform_all(validate_first=True)

    assert "Bronze validation GATE FAILED" in str(exc_info.value)
    # Ensure no Silver products file was produced
    assert not (silver_dir / "products.csv").exists()


def test_idempotency_and_repeat_runs(pipeline_dirs):
    """Verifies that running Silver transformations repeatedly produces equivalent outputs."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)

    summary_1 = transformer.transform_all(validate_first=False)
    output_1 = {f: (silver_dir / f).read_bytes() for f in EXPECTED_DATASETS}

    summary_2 = transformer.transform_all(validate_first=False)
    output_2 = {f: (silver_dir / f).read_bytes() for f in EXPECTED_DATASETS}

    assert summary_1["total_output_records"] == summary_2["total_output_records"]
    assert output_1 == output_2


def test_source_and_bronze_immutability(pipeline_dirs):
    """Guarantees that raw and Bronze data are never modified during Silver transformation."""
    raw_dir = pipeline_dirs["raw_dir"]
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    raw_hashes_before = {f.name: f.read_bytes() for f in raw_dir.iterdir() if f.suffix == ".csv"}
    bronze_hashes_before = {f.name: f.read_bytes() for f in bronze_dir.iterdir() if f.suffix == ".csv"}

    transformer = SilverDataTransformer(bronze_dir=bronze_dir, silver_dir=silver_dir)
    transformer.transform_all(validate_first=True)

    raw_hashes_after = {f.name: f.read_bytes() for f in raw_dir.iterdir() if f.suffix == ".csv"}
    bronze_hashes_after = {f.name: f.read_bytes() for f in bronze_dir.iterdir() if f.suffix == ".csv"}

    assert raw_hashes_before == raw_hashes_after, "Raw data was modified by Silver transformation!"
    assert bronze_hashes_before == bronze_hashes_after, "Bronze data was modified by Silver transformation!"


def test_cli_execution(pipeline_dirs, monkeypatch):
    """Verifies CLI execution with Bronze pre-validation."""
    bronze_dir = pipeline_dirs["bronze_dir"]
    silver_dir = pipeline_dirs["silver_dir"]

    test_args = [
        "transform_silver.py",
        "--bronze-dir", str(bronze_dir),
        "--silver-dir", str(silver_dir),
    ]
    monkeypatch.setattr("sys.argv", test_args)

    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0
    assert (silver_dir / "suppliers.csv").exists()
    assert (silver_dir / "transformation_report.json").exists()
