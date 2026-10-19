"""Automated test suite for RetailFlow Data Quality and Contracts validation."""

import csv
import json
from pathlib import Path
import pytest

from src.data_generator.generate_data import RetailDataGenerator
from src.quality.contracts import ALL_CONTRACTS
from src.quality.validator import (
    CheckSeverity,
    CheckStatus,
    DataQualityValidator,
    main,
)


@pytest.fixture
def clean_test_data(tmp_path):
    """Generates a clean, valid relational dataset in a temporary directory."""
    raw_dir = tmp_path / "raw"
    sample_dir = tmp_path / "sample"

    generator = RetailDataGenerator(
        seed=42,
        output_dir=str(raw_dir),
        sample_dir=str(sample_dir),
    )
    generator.run_all(num_suppliers=5, num_products=15, num_orders=25)
    return raw_dir


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


def test_data_quality_on_clean_data_passes(clean_test_data):
    """Verifies that clean synthetic data passes all critical quality checks."""
    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "PASSED"
    assert report.failed_checks == 0
    assert report.critical_errors == 0
    assert report.total_checks > 100
    assert report.passed_checks > 0


def test_missing_required_column_fails(clean_test_data):
    """Verifies that omitting a required column fails validation."""
    products_file = clean_test_data / "products.csv"
    rows = read_csv_rows(products_file)

    # Remove 'unit_price' column from header and rows
    for r in rows:
        r.pop("unit_price", None)
    new_fields = [k for k in rows[0].keys()]
    write_csv_rows(products_file, rows, new_fields)

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    failed_names = [c.check_name for c in report.checks if c.status == CheckStatus.FAILED]
    assert any("Required Columns Check" in name for name in failed_names)


def test_empty_source_file_fails(clean_test_data):
    """Verifies that an empty (0 bytes) dataset triggers a critical failure."""
    suppliers_file = clean_test_data / "suppliers.csv"
    suppliers_file.write_text("", encoding="utf-8")

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    failed_checks = [c for c in report.checks if c.status == CheckStatus.FAILED and c.dataset == "suppliers.csv"]
    assert any("Non-Empty File Check" in c.check_name for c in failed_checks)


def test_missing_dataset_file_fails(clean_test_data):
    """Verifies that a missing dataset triggers a critical failure."""
    warehouses_file = clean_test_data / "warehouses.csv"
    warehouses_file.unlink()

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    failed_checks = [c for c in report.checks if c.status == CheckStatus.FAILED and c.dataset == "warehouses.csv"]
    assert any("File Presence Check" in c.check_name for c in failed_checks)


def test_duplicate_primary_key_fails(clean_test_data):
    """Verifies that duplicate primary keys trigger a critical failure."""
    suppliers_file = clean_test_data / "suppliers.csv"
    rows = read_csv_rows(suppliers_file)

    # Duplicate first supplier ID
    rows[1]["supplier_id"] = rows[0]["supplier_id"]
    write_csv_rows(suppliers_file, rows, list(rows[0].keys()))

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    dup_check = next(c for c in report.checks if c.check_id == "suppliers.csv_primary_key_uniqueness")
    assert dup_check.status == CheckStatus.FAILED
    assert dup_check.failed_records == 1


def test_broken_foreign_key_fails(clean_test_data):
    """Verifies that orphan foreign keys trigger critical referential integrity failure."""
    inv_file = clean_test_data / "inventory.csv"
    rows = read_csv_rows(inv_file)

    # Point first inventory record to non-existent warehouse
    rows[0]["warehouse_id"] = "WH999_ORPHAN"
    write_csv_rows(inv_file, rows, list(rows[0].keys()))

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    fk_check = next(c for c in report.checks if c.check_id == "inventory.csv_warehouse_id_foreign_key")
    assert fk_check.status == CheckStatus.FAILED
    assert fk_check.failed_records == 1
    assert fk_check.sample_issues[0]["missing_parent_id"] == "WH999_ORPHAN"


def test_invalid_data_types_and_constraints_fail(clean_test_data):
    """Verifies that non-parsable numbers or out-of-bounds values fail column contracts."""
    suppliers_file = clean_test_data / "suppliers.csv"
    rows = read_csv_rows(suppliers_file)

    # Non-integer lead_time_days and invalid reliability_score > 1.0
    rows[0]["lead_time_days"] = "NOT_A_NUMBER"
    rows[1]["reliability_score"] = "2.5"
    write_csv_rows(suppliers_file, rows, list(rows[0].keys()))

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    type_check = next(c for c in report.checks if c.check_id == "suppliers.csv_lead_time_days_type")
    assert type_check.status == CheckStatus.FAILED
    assert type_check.failed_records == 1

    constraint_check = next(c for c in report.checks if c.check_id == "suppliers.csv_reliability_score_constraints")
    assert constraint_check.status == CheckStatus.FAILED
    assert constraint_check.failed_records == 1


def test_business_rule_inventory_negative_stock_fails(clean_test_data):
    """Verifies that negative inventory stock values are rejected."""
    inv_file = clean_test_data / "inventory.csv"
    rows = read_csv_rows(inv_file)

    rows[0]["stock_on_hand"] = "-15"
    write_csv_rows(inv_file, rows, list(rows[0].keys()))

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    check = next(c for c in report.checks if c.check_id == "inventory_non_negative_quantities")
    assert check.status == CheckStatus.FAILED
    assert check.failed_records == 1


def test_business_rule_reserved_exceeds_on_hand_fails(clean_test_data):
    """Verifies that stock_reserved exceeding physical stock_on_hand is rejected."""
    inv_file = clean_test_data / "inventory.csv"
    rows = read_csv_rows(inv_file)

    rows[0]["stock_on_hand"] = "5"
    rows[0]["stock_reserved"] = "20"
    write_csv_rows(inv_file, rows, list(rows[0].keys()))

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    check = next(c for c in report.checks if c.check_id == "inventory_reserved_le_on_hand")
    assert check.status == CheckStatus.FAILED
    assert check.failed_records == 1


def test_business_rule_received_without_delivery_date_fails(clean_test_data):
    """Verifies that orders marked RECEIVED without actual delivery dates fail."""
    po_file = clean_test_data / "purchase_orders.csv"
    rows = read_csv_rows(po_file)

    # Find a RECEIVED order and remove its delivery date
    rec_order = next(r for r in rows if r["order_status"] == "RECEIVED")
    rec_order["actual_delivery_date"] = ""
    write_csv_rows(po_file, rows, list(rows[0].keys()))

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    check = next(c for c in report.checks if c.check_id == "po_received_has_actual_delivery_date")
    assert check.status == CheckStatus.FAILED
    assert check.failed_records >= 1


def test_business_rule_cancelled_pending_with_delivery_date_fails(clean_test_data):
    """Verifies that orders marked PENDING or CANCELLED with actual delivery dates fail."""
    po_file = clean_test_data / "purchase_orders.csv"
    rows = read_csv_rows(po_file)

    # Set delivery date on a PENDING order
    pending_order = next(r for r in rows if r["order_status"] == "PENDING")
    pending_order["actual_delivery_date"] = "2026-05-15"
    write_csv_rows(po_file, rows, list(rows[0].keys()))

    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    assert report.overall_status == "FAILED"
    check = next(c for c in report.checks if c.check_id == "po_pending_cancelled_not_received_date")
    assert check.status == CheckStatus.FAILED
    assert check.failed_records >= 1


def test_warnings_distinguished_from_critical_errors(clean_test_data):
    """Verifies that normal business events (delayed delivery, stockout) are warnings, not critical errors."""
    validator = DataQualityValidator(data_dir=clean_test_data)
    report = validator.validate_all()

    # The clean test data has delayed shipments and stockout scenarios generated intentionally
    warning_checks = [c for c in report.checks if c.severity == CheckSeverity.WARNING]
    assert len(warning_checks) > 0

    # None of the warnings should cause failed_checks or FAILED overall status
    assert report.failed_checks == 0
    assert report.overall_status == "PASSED"
    assert "total_delayed_orders" in report.metrics


def test_source_data_immutability_during_validation(clean_test_data):
    """Guarantees that validating datasets never modifies or deletes raw files."""
    pre_hashes = {
        f.name: f.read_bytes()
        for f in clean_test_data.iterdir()
        if f.suffix == ".csv"
    }

    validator = DataQualityValidator(data_dir=clean_test_data)
    validator.validate_all()

    post_hashes = {
        f.name: f.read_bytes()
        for f in clean_test_data.iterdir()
        if f.suffix == ".csv"
    }

    assert pre_hashes == post_hashes, "Data files were modified during quality audit!"


def test_cli_exit_codes(clean_test_data, monkeypatch, tmp_path):
    """Verifies CLI exit code 0 for passing data and code 1 for failing data."""
    output_report = tmp_path / "report.json"

    # Test 1: Clean data -> exit 0
    test_args_clean = [
        "validator.py",
        "--data-dir", str(clean_test_data),
        "--output", str(output_report),
    ]
    monkeypatch.setattr("sys.argv", test_args_clean)

    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0
    assert output_report.exists()

    # Test 2: Broken data -> exit 1
    (clean_test_data / "suppliers.csv").write_text("", encoding="utf-8")
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 1
