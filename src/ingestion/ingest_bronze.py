"""Bronze Data Ingestion for RetailFlow Data Engineering Pipeline.

Medallion Architecture - Bronze Layer:
The Bronze layer represents the raw landing zone for enterprise data.
Its fundamental rule is RAW FIDELITY:
- Source column names and order are preserved exactly as received.
- Source record values, whitespace, and types are never altered or cast.
- No deduplication, business filtering, or cleaning occurs here.
- Source files in `data/raw/` are treated as strictly immutable (read-only).
- An audit manifest (`ingestion_manifest.json`) is generated in UTC.
"""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

# Define PROJECT_ROOT as the root of the retailflow repository
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger

# Standard retail supply chain tables generated in Phase 1
DEFAULT_SOURCE_FILES = [
    "suppliers.csv",
    "warehouses.csv",
    "products.csv",
    "inventory.csv",
    "purchase_orders.csv",
    "purchase_order_items.csv",
]


class IngestionError(Exception):
    """Base exception for all RetailFlow ingestion errors."""
    pass


class MissingSourceFileError(IngestionError):
    """Raised when an expected source file is missing from the raw directory."""
    pass


class EmptySourceFileError(IngestionError):
    """Raised when a source file is completely empty (0 bytes) or contains only headers."""
    pass


class MalformedCSVError(IngestionError):
    """Raised when a CSV source file cannot be parsed or contains corrupt rows."""
    pass


class IngestionWriteError(IngestionError):
    """Raised when writing the bronze file or metadata manifest fails."""
    pass


class BronzeDataIngestion:
    """Manages the raw-to-bronze ingestion pipeline for RetailFlow datasets.

    Attributes:
        raw_dir: Path to the raw source data directory (read-only).
        bronze_dir: Path to the target bronze landing directory.
        manifest_filename: Name of the JSON metadata manifest file.
        expected_files: List of expected CSV filenames to ingest.
        logger: Configured logging instance.
    """

    def __init__(
        self,
        raw_dir: Optional[Union[str, Path]] = None,
        bronze_dir: Optional[Union[str, Path]] = None,
        manifest_filename: str = "ingestion_manifest.json",
        expected_files: Optional[List[str]] = None,
    ):
        """Initializes the BronzeDataIngestion service with path safeguards.

        Args:
            raw_dir: Source raw directory path. Defaults to `<project_root>/data/raw`.
            bronze_dir: Target bronze directory path. Defaults to `<project_root>/data/bronze`.
            manifest_filename: File name for the JSON audit manifest.
            expected_files: List of required CSV filenames. Defaults to DEFAULT_SOURCE_FILES.

        Raises:
            ValueError: If bronze_dir is located inside src/ or matches raw_dir.
        """
        self.logger = get_logger("retailflow.ingestion")

        # Resolve paths relative to PROJECT_ROOT if relative paths provided
        default_raw = PROJECT_ROOT / "data" / "raw"
        default_bronze = PROJECT_ROOT / "data" / "bronze"

        if raw_dir is None:
            self.raw_dir = default_raw
        else:
            p_raw = Path(raw_dir)
            self.raw_dir = p_raw if p_raw.is_absolute() else (PROJECT_ROOT / p_raw).resolve()

        if bronze_dir is None:
            self.bronze_dir = default_bronze
        else:
            p_bronze = Path(bronze_dir)
            self.bronze_dir = p_bronze if p_bronze.is_absolute() else (PROJECT_ROOT / p_bronze).resolve()

        # Path safeguards
        src_dir = (PROJECT_ROOT / "src").resolve()
        if self.bronze_dir.resolve().is_relative_to(src_dir):
            raise ValueError(f"Bronze directory cannot be located inside src/: {self.bronze_dir}")
        if self.bronze_dir.resolve() == self.raw_dir.resolve():
            raise ValueError(f"Bronze directory cannot be identical to raw directory: {self.bronze_dir}")

        self.manifest_filename = manifest_filename
        self.expected_files = list(expected_files) if expected_files is not None else list(DEFAULT_SOURCE_FILES)

        # Ensure bronze output directory exists
        self.bronze_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def compute_sha256(file_path: Path) -> str:
        """Computes SHA-256 checksum of a file for audit and integrity verification.

        Args:
            file_path: Path of the file to hash.

        Returns:
            Hex-encoded SHA-256 digest string.
        """
        hasher = hashlib.sha256()
        with open(file_path, mode="rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        return hasher.hexdigest()

    def validate_source_file(self, filename: str) -> Tuple[Path, List[str], int]:
        """Validates that a source CSV file exists, is non-empty, and has valid syntax.

        Args:
            filename: Name of the CSV file in the raw directory.

        Returns:
            Tuple of (resolved_source_path, column_names, row_count).

        Raises:
            MissingSourceFileError: If the file does not exist.
            EmptySourceFileError: If file is 0 bytes or has 0 data rows.
            MalformedCSVError: If CSV syntax or field count is invalid.
        """
        source_path = self.raw_dir / filename

        # 1. Check existence
        if not source_path.exists() or not source_path.is_file():
            self.logger.error("Missing source file: %s at %s", filename, source_path)
            raise MissingSourceFileError(f"Expected source file not found: '{filename}' at {source_path}")

        # 2. Check 0 bytes
        file_size = source_path.stat().st_size
        if file_size == 0:
            self.logger.error("Source file is empty (0 bytes): %s", filename)
            raise EmptySourceFileError(f"Source file '{filename}' is empty (0 bytes).")

        # 3. Read and parse CSV safely
        try:
            with open(source_path, mode="r", encoding="utf-8", newline="") as f:
                reader = csv.reader(f)

                try:
                    header = next(reader)
                except StopIteration:
                    self.logger.error("Source file has no content: %s", filename)
                    raise EmptySourceFileError(f"Source file '{filename}' contains no content.")

                if not header or all(col.strip() == "" for col in header):
                    self.logger.error("Source file has empty header: %s", filename)
                    raise EmptySourceFileError(f"Source file '{filename}' contains an empty header row.")

                expected_num_cols = len(header)
                row_count = 0

                for line_idx, row in enumerate(reader, start=2):
                    if len(row) != expected_num_cols:
                        self.logger.error(
                            "Malformed CSV row in %s at line %d: expected %d columns, got %d",
                            filename,
                            line_idx,
                            expected_num_cols,
                            len(row),
                        )
                        raise MalformedCSVError(
                            f"Malformed CSV in '{filename}' at line {line_idx}: "
                            f"expected {expected_num_cols} columns, found {len(row)}."
                        )
                    row_count += 1

                if row_count == 0:
                    self.logger.error("Source file contains header only with 0 records: %s", filename)
                    raise EmptySourceFileError(
                        f"Source file '{filename}' contains only a header row with 0 data records."
                    )

                return source_path, header, row_count

        except UnicodeDecodeError as exc:
            self.logger.error("Encoding error reading source file %s: %s", filename, exc)
            raise MalformedCSVError(f"Source file '{filename}' has invalid UTF-8 encoding: {exc}") from exc
        except csv.Error as exc:
            self.logger.error("CSV parser error in %s: %s", filename, exc)
            raise MalformedCSVError(f"Source file '{filename}' contains invalid CSV formatting: {exc}") from exc

    def ingest_single_file(self, filename: str) -> Dict[str, Any]:
        """Ingests a single validated CSV into the Bronze layer and captures metadata.

        Preserves raw data verbatim without altering values, columns, or rows.
        Uses atomic file replacement to prevent partially written files.

        Args:
            filename: Name of the CSV file to ingest.

        Returns:
            Dictionary containing audit metadata for the ingested file.

        Raises:
            IngestionWriteError: If destination write fails.
        """
        source_path, columns, row_count = self.validate_source_file(filename)
        target_path = self.bronze_dir / filename
        temp_target = self.bronze_dir / f"{filename}.tmp_{uuid.uuid4().hex[:8]}"

        self.logger.info(
            "Ingesting %s: %d records, %d columns from %s",
            filename,
            row_count,
            len(columns),
            source_path,
        )

        try:
            # Atomic copy: write to temp file first, then replace
            shutil.copyfile(source_path, temp_target)
            temp_target.replace(target_path)
        except Exception as exc:
            if temp_target.exists():
                temp_target.unlink()
            self.logger.error("Failed writing bronze file %s: %s", target_path, exc)
            raise IngestionWriteError(f"Failed to write bronze file '{target_path}': {exc}") from exc

        # Timestamp in UTC ISO 8601
        ingestion_time_utc = datetime.now(timezone.utc).isoformat()
        sha256_hash = self.compute_sha256(target_path)

        metadata = {
            "source_filename": filename,
            "source_path": str(source_path.resolve()),
            "bronze_path": str(target_path.resolve()),
            "row_count": row_count,
            "column_count": len(columns),
            "column_names": columns,
            "file_size_bytes": target_path.stat().st_size,
            "sha256": sha256_hash,
            "ingestion_timestamp_utc": ingestion_time_utc,
            "status": "SUCCESS",
        }

        return metadata

    def write_manifest(self, batch_id: str, batch_metadata: Dict[str, Any]) -> Path:
        """Writes the batch audit manifest to JSON atomically.

        Args:
            batch_id: Unique batch execution ID.
            batch_metadata: Metadata dictionary containing details of all ingested files.

        Returns:
            Path to the written manifest file.

        Raises:
            IngestionWriteError: If writing the manifest fails.
        """
        manifest_path = self.bronze_dir / self.manifest_filename
        temp_manifest = self.bronze_dir / f"{self.manifest_filename}.tmp_{batch_id}"

        manifest_payload = {
            "batch_id": batch_id,
            "ingestion_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "source_directory": str(self.raw_dir.resolve()),
            "bronze_directory": str(self.bronze_dir.resolve()),
            "total_files": len(batch_metadata),
            "total_records": sum(m["row_count"] for m in batch_metadata.values()),
            "status": "SUCCESS",
            "files": batch_metadata,
        }

        try:
            with open(temp_manifest, mode="w", encoding="utf-8") as f:
                json.dump(manifest_payload, f, indent=2)
            temp_manifest.replace(manifest_path)
            self.logger.info("Ingestion manifest successfully written to %s", manifest_path)
            return manifest_path
        except Exception as exc:
            if temp_manifest.exists():
                temp_manifest.unlink()
            self.logger.error("Failed to write ingestion manifest: %s", exc)
            raise IngestionWriteError(f"Failed to write ingestion manifest to '{manifest_path}': {exc}") from exc

    def ingest_all(self) -> Dict[str, Any]:
        """Executes full Bronze ingestion for all expected files.

        Idempotency / Reruns:
            Repeated runs are safe. Existing files in `data/bronze/` are overwritten
            with fresh raw copies atomically, and the manifest is updated with the
            new timestamp and batch ID. The raw source files in `data/raw/` are NEVER
            modified or overwritten.

        Returns:
            Dictionary containing the completed batch manifest summary.

        Raises:
            MissingSourceFileError: If any expected source file is missing.
            EmptySourceFileError: If any source file has no data.
            MalformedCSVError: If any CSV parsing fails.
            IngestionWriteError: If file or manifest writes fail.
        """
        batch_id = str(uuid.uuid4())
        self.logger.info("Starting Bronze ingestion batch %s", batch_id)

        batch_metadata: Dict[str, Any] = {}

        for filename in self.expected_files:
            file_meta = self.ingest_single_file(filename)
            batch_metadata[filename] = file_meta

        manifest_path = self.write_manifest(batch_id, batch_metadata)

        summary = {
            "batch_id": batch_id,
            "status": "SUCCESS",
            "manifest_path": str(manifest_path.resolve()),
            "files_ingested": len(batch_metadata),
            "total_records": sum(m["row_count"] for m in batch_metadata.values()),
        }

        self.logger.info(
            "Bronze ingestion completed successfully. Batch: %s, Ingested %d files, %d total records.",
            batch_id,
            summary["files_ingested"],
            summary["total_records"],
        )
        return summary


def main():
    """CLI entrypoint for running Bronze Data Ingestion."""
    parser = argparse.ArgumentParser(description="RetailFlow Bronze Layer Data Ingestion")
    parser.add_argument(
        "--raw-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "raw"),
        help="Path to raw source CSV directory",
    )
    parser.add_argument(
        "--bronze-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "bronze"),
        help="Path to bronze target landing directory",
    )
    parser.add_argument(
        "--manifest-name",
        type=str,
        default="ingestion_manifest.json",
        help="File name for JSON ingestion manifest",
    )

    args = parser.parse_args()

    print(f"Starting RetailFlow Bronze Data Ingestion...")
    print(f"  Source (Raw):  {args.raw_dir}")
    print(f"  Target (Bronze): {args.bronze_dir}")

    ingestion = BronzeDataIngestion(
        raw_dir=args.raw_dir,
        bronze_dir=args.bronze_dir,
        manifest_filename=args.manifest_name,
    )

    summary = ingestion.ingest_all()

    print("\nBronze Ingestion Successful!")
    print(f"  Batch ID:       {summary['batch_id']}")
    print(f"  Files Ingested: {summary['files_ingested']}")
    print(f"  Total Records:  {summary['total_records']}")
    print(f"  Manifest Path:  {summary['manifest_path']}")


if __name__ == "__main__":
    main()
