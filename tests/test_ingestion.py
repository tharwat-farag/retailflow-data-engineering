"""Automated test suite for RetailFlow Bronze Data Ingestion."""

import csv
from datetime import datetime
import json
from pathlib import Path
import pytest

from src.data_generator.generate_data import RetailDataGenerator
from src.ingestion.ingest_bronze import (
    DEFAULT_SOURCE_FILES,
    BronzeDataIngestion,
    EmptySourceFileError,
    IngestionWriteError,
    MalformedCSVError,
    MissingSourceFileError,
)


@pytest.fixture
def test_dirs(tmp_path):
    """Creates isolated raw and bronze temporary directories with generated test data."""
    raw_dir = tmp_path / "raw"
    sample_dir = tmp_path / "sample"
    bronze_dir = tmp_path / "bronze"

    generator = RetailDataGenerator(
        seed=123,
        output_dir=str(raw_dir),
        sample_dir=str(sample_dir),
    )
    generator.run_all(num_suppliers=5, num_products=15, num_orders=30)

    return {
        "raw_dir": raw_dir,
        "bronze_dir": bronze_dir,
    }


def test_successful_ingestion_all_datasets(test_dirs):
    """Verifies that all 6 expected source datasets are successfully ingested into data/bronze/."""
    raw_dir = test_dirs["raw_dir"]
    bronze_dir = test_dirs["bronze_dir"]

    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)
    summary = ingestion.ingest_all()

    assert summary["status"] == "SUCCESS"
    assert summary["files_ingested"] == len(DEFAULT_SOURCE_FILES)
    assert summary["total_records"] > 0

    # Verify each bronze file exists and is identical to raw file
    for filename in DEFAULT_SOURCE_FILES:
        raw_file = raw_dir / filename
        bronze_file = bronze_dir / filename

        assert bronze_file.exists(), f"Bronze file missing: {filename}"
        assert bronze_file.stat().st_size > 0

        # Raw fidelity: verify exact byte match
        assert raw_file.read_bytes() == bronze_file.read_bytes(), (
            f"Bronze file content diverges from raw: {filename}"
        )


def test_manifest_metadata_structure_and_values(test_dirs):
    """Verifies manifest schema, UTC timestamps, record counts, and column preservation."""
    raw_dir = test_dirs["raw_dir"]
    bronze_dir = test_dirs["bronze_dir"]

    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)
    summary = ingestion.ingest_all()

    manifest_path = bronze_dir / "ingestion_manifest.json"
    assert manifest_path.exists()
    assert summary["manifest_path"] == str(manifest_path.resolve())

    with open(manifest_path, mode="r", encoding="utf-8") as f:
        manifest = json.load(f)

    # Check top-level manifest keys
    assert "batch_id" in manifest
    assert "ingestion_timestamp_utc" in manifest
    assert "source_directory" in manifest
    assert "bronze_directory" in manifest
    assert manifest["status"] == "SUCCESS"
    assert manifest["total_files"] == 6

    # Verify timestamp is valid UTC ISO 8601
    batch_ts = datetime.fromisoformat(manifest["ingestion_timestamp_utc"])
    assert batch_ts.tzinfo is not None

    # Check file-level metadata
    for filename in DEFAULT_SOURCE_FILES:
        assert filename in manifest["files"]
        meta = manifest["files"][filename]

        assert meta["source_filename"] == filename
        assert Path(meta["source_path"]).exists()
        assert Path(meta["bronze_path"]).exists()
        assert meta["row_count"] > 0
        assert meta["column_count"] == len(meta["column_names"])
        assert meta["file_size_bytes"] > 0
        assert len(meta["sha256"]) == 64  # valid sha256 hex string
        assert meta["status"] == "SUCCESS"

        # Verify actual CSV rows match row_count
        with open(bronze_dir / filename, mode="r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            actual_rows = sum(1 for _ in reader)

        assert meta["column_names"] == header
        assert meta["row_count"] == actual_rows


def test_missing_source_file_raises_clear_error(test_dirs):
    """Verifies that missing source files raise MissingSourceFileError and are not silently skipped."""
    raw_dir = test_dirs["raw_dir"]
    bronze_dir = test_dirs["bronze_dir"]

    # Delete one required file
    missing_file = raw_dir / "products.csv"
    missing_file.unlink()

    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)

    with pytest.raises(MissingSourceFileError) as exc_info:
        ingestion.ingest_all()

    assert "products.csv" in str(exc_info.value)
    assert "Expected source file not found" in str(exc_info.value)


def test_empty_source_file_zero_bytes_raises_error(test_dirs):
    """Verifies that a 0-byte source file raises EmptySourceFileError with clear message."""
    raw_dir = test_dirs["raw_dir"]
    bronze_dir = test_dirs["bronze_dir"]

    # Overwrite one file with 0 bytes
    empty_file = raw_dir / "suppliers.csv"
    empty_file.write_text("", encoding="utf-8")

    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)

    with pytest.raises(EmptySourceFileError) as exc_info:
        ingestion.ingest_all()

    assert "suppliers.csv" in str(exc_info.value)
    assert "0 bytes" in str(exc_info.value)


def test_empty_source_file_header_only_raises_error(test_dirs):
    """Verifies that a file containing only a header row and 0 data rows raises EmptySourceFileError."""
    raw_dir = test_dirs["raw_dir"]
    bronze_dir = test_dirs["bronze_dir"]

    # Overwrite one file with header only
    header_only_file = raw_dir / "warehouses.csv"
    header_only_file.write_text("warehouse_id,name,city\n", encoding="utf-8")

    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)

    with pytest.raises(EmptySourceFileError) as exc_info:
        ingestion.ingest_all()

    assert "warehouses.csv" in str(exc_info.value)
    assert "0 data records" in str(exc_info.value)


def test_malformed_csv_column_mismatch_raises_error(test_dirs):
    """Verifies that a corrupted CSV with inconsistent column counts raises MalformedCSVError."""
    raw_dir = test_dirs["raw_dir"]
    bronze_dir = test_dirs["bronze_dir"]

    # Inject malformed line (missing columns or excess unescaped commas)
    target_file = raw_dir / "products.csv"
    lines = target_file.read_text(encoding="utf-8").splitlines()
    lines[2] = "corrupt,row,only_three_cols"  # Expected 8 columns
    target_file.write_text("\n".join(lines), encoding="utf-8")

    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)

    with pytest.raises(MalformedCSVError) as exc_info:
        ingestion.ingest_all()

    assert "products.csv" in str(exc_info.value)
    assert "Malformed CSV" in str(exc_info.value)
    assert "expected 8 columns" in str(exc_info.value)


def test_rerun_safety_and_idempotency(test_dirs):
    """Verifies that repeated ingestion runs are idempotent and safely overwrite bronze outputs."""
    raw_dir = test_dirs["raw_dir"]
    bronze_dir = test_dirs["bronze_dir"]

    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)

    # First run
    summary_1 = ingestion.ingest_all()
    assert summary_1["status"] == "SUCCESS"

    with open(bronze_dir / "ingestion_manifest.json", encoding="utf-8") as f:
        manifest_1 = json.load(f)

    # Second run
    summary_2 = ingestion.ingest_all()
    assert summary_2["status"] == "SUCCESS"

    with open(bronze_dir / "ingestion_manifest.json", encoding="utf-8") as f:
        manifest_2 = json.load(f)

    # Batch IDs and timestamps should update
    assert manifest_1["batch_id"] != manifest_2["batch_id"]
    assert manifest_2["ingestion_timestamp_utc"] >= manifest_1["ingestion_timestamp_utc"]

    # Ingested record counts and content must match exactly
    assert manifest_1["total_records"] == manifest_2["total_records"]
    for filename in DEFAULT_SOURCE_FILES:
        assert (bronze_dir / filename).exists()
        assert (raw_dir / filename).read_bytes() == (bronze_dir / filename).read_bytes()


def test_raw_files_never_modified_during_ingestion(test_dirs):
    """Guarantees that raw source files are strictly read-only and never modified by ingestion."""
    raw_dir = test_dirs["raw_dir"]
    bronze_dir = test_dirs["bronze_dir"]

    # Compute raw hashes prior to ingestion
    pre_hashes = {
        fname: BronzeDataIngestion.compute_sha256(raw_dir / fname)
        for fname in DEFAULT_SOURCE_FILES
    }

    ingestion = BronzeDataIngestion(raw_dir=raw_dir, bronze_dir=bronze_dir)
    ingestion.ingest_all()

    # Compute raw hashes after ingestion
    post_hashes = {
        fname: BronzeDataIngestion.compute_sha256(raw_dir / fname)
        for fname in DEFAULT_SOURCE_FILES
    }

    assert pre_hashes == post_hashes, "Raw source files were altered during ingestion!"


def test_path_safeguards():
    """Ensures BronzeDataIngestion rejects bronze destinations inside src/ or matching raw_dir."""
    from src.ingestion.ingest_bronze import PROJECT_ROOT

    # Attempting to place bronze inside src/
    with pytest.raises(ValueError, match="Bronze directory cannot be located inside src/"):
        BronzeDataIngestion(bronze_dir=PROJECT_ROOT / "src" / "bronze")

    # Attempting to set bronze directory identical to raw directory
    with pytest.raises(ValueError, match="cannot be identical to raw directory"):
        BronzeDataIngestion(raw_dir="data/raw", bronze_dir="data/raw")


def test_cli_main_execution(test_dirs, monkeypatch, capsys):
    """Verifies that the CLI entrypoint runs correctly with custom argument parsing."""
    from src.ingestion.ingest_bronze import main

    test_args = [
        "ingest_bronze.py",
        "--raw-dir",
        str(test_dirs["raw_dir"]),
        "--bronze-dir",
        str(test_dirs["bronze_dir"]),
        "--manifest-name",
        "cli_manifest.json",
    ]
    monkeypatch.setattr("sys.argv", test_args)

    main()

    captured = capsys.readouterr()
    assert "Bronze Ingestion Successful!" in captured.out
    assert (test_dirs["bronze_dir"] / "cli_manifest.json").exists()
