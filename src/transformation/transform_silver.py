"""Silver Layer Transformations for RetailFlow Data Engineering Pipeline.

Medallion Architecture - Silver Layer:
The Silver layer cleans, conforms, enriches, and validates Bronze datasets:
- Standardizes text fields, whitespace, date formats, and numeric precisions.
- Preserves referential integrity and original identifier schemas.
- Handles duplicates deterministically and quarantines conflicting or corrupted records.
- Preserves core business meaning (e.g. pending/cancelled orders are never treated as received).
- Preserves raw and Bronze source data strictly immutable.
- Halts execution and exits non-zero if upstream Bronze data quality fails critical contracts.
- Generates a detailed audit transformation report with quarantine logs.
"""

import argparse
import csv
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import shutil
import sys
from typing import Any, Dict, List, Optional, Set, Tuple, Union
import uuid

# Define PROJECT_ROOT and configure sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality.validator import CheckSeverity, CheckStatus, DataQualityValidator
from src.utils.logger import get_logger

# Standard expected supply chain tables
EXPECTED_DATASETS = [
    "suppliers.csv",
    "warehouses.csv",
    "products.csv",
    "inventory.csv",
    "purchase_orders.csv",
    "purchase_order_items.csv",
]


class TransformationError(Exception):
    """Base exception for all Silver transformation errors."""
    pass


class ValidationHaltError(TransformationError):
    """Raised when upstream Bronze validation fails critical checks, halting transformation."""
    pass


class SilverWriteError(TransformationError):
    """Raised when writing Silver dataset or quarantine records fails."""
    pass


@dataclass
class QuarantinedRecord:
    """Represents a record that could not be safely promoted to Silver.

    Attributes:
        dataset: Dataset filename.
        primary_key_name: Name of PK column.
        primary_key_value: Value of PK if available.
        reason: Categorical reason code (e.g. 'FOREIGN_KEY_VIOLATION', 'CONFLICTING_DUPLICATE').
        details: Actionable diagnosis of why the record was quarantined.
        raw_record: Original raw row values.
        quarantine_timestamp_utc: When the record was quarantined.
    """
    dataset: str
    primary_key_name: str
    primary_key_value: str
    reason: str
    details: str
    raw_record: Dict[str, Any]
    quarantine_timestamp_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class SilverDataTransformer:
    """Executes conformed Silver transformations on validated Bronze supply chain datasets."""

    def __init__(
        self,
        bronze_dir: Optional[Union[str, Path]] = None,
        silver_dir: Optional[Union[str, Path]] = None,
        quarantine_dir: Optional[Union[str, Path]] = None,
        report_filename: str = "transformation_report.json",
    ):
        """Initializes the SilverDataTransformer with path safeguards.

        Args:
            bronze_dir: Path to source Bronze directory. Defaults to `<project_root>/data/bronze`.
            silver_dir: Path to target Silver directory. Defaults to `<project_root>/data/silver`.
            quarantine_dir: Path to quarantine directory. Defaults to `<silver_dir>/quarantine`.
            report_filename: Name of the JSON transformation report file.

        Raises:
            ValueError: If silver_dir is placed inside src/ or matches bronze/raw directory.
        """
        self.logger = get_logger("retailflow.transformation")

        default_bronze = PROJECT_ROOT / "data" / "bronze"
        default_silver = PROJECT_ROOT / "data" / "silver"

        if bronze_dir is None:
            self.bronze_dir = default_bronze
        else:
            p_b = Path(bronze_dir)
            self.bronze_dir = p_b if p_b.is_absolute() else (PROJECT_ROOT / p_b).resolve()

        if silver_dir is None:
            self.silver_dir = default_silver
        else:
            p_s = Path(silver_dir)
            self.silver_dir = p_s if p_s.is_absolute() else (PROJECT_ROOT / p_s).resolve()

        # Path safeguards
        src_dir = (PROJECT_ROOT / "src").resolve()
        raw_dir = (PROJECT_ROOT / "data" / "raw").resolve()

        if self.silver_dir.resolve().is_relative_to(src_dir):
            raise ValueError(f"Silver directory cannot be located inside src/: {self.silver_dir}")
        if self.silver_dir.resolve() == self.bronze_dir.resolve():
            raise ValueError(f"Silver directory cannot be identical to bronze directory: {self.silver_dir}")
        if self.silver_dir.resolve() == raw_dir:
            raise ValueError(f"Silver directory cannot be identical to raw directory: {self.silver_dir}")

        if quarantine_dir is None:
            self.quarantine_dir = self.silver_dir / "quarantine"
        else:
            p_q = Path(quarantine_dir)
            self.quarantine_dir = p_q if p_q.is_absolute() else (PROJECT_ROOT / p_q).resolve()

        self.report_filename = report_filename
        self.quarantined_records: List[QuarantinedRecord] = []
        self.dedup_stats: Dict[str, int] = {ds: 0 for ds in EXPECTED_DATASETS}

        # Ensure output directories exist
        self.silver_dir.mkdir(parents=True, exist_ok=True)
        self.quarantine_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def clean_text(val: Optional[str]) -> str:
        """Trims leading/trailing whitespace and normalizes internal whitespace."""
        if val is None:
            return ""
        return re.sub(r"\s+", " ", val.strip())

    @staticmethod
    def parse_iso_date(val: Optional[str]) -> str:
        """Validates and normalizes date string to ISO YYYY-MM-DD format."""
        if not val or not val.strip():
            return ""
        clean = val.strip()
        dt = datetime.strptime(clean, "%Y-%m-%d").date()
        return dt.isoformat()

    def _read_bronze_csv(self, filename: str) -> List[Dict[str, str]]:
        """Reads a Bronze CSV file strictly in read-only mode."""
        file_path = self.bronze_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Bronze file '{filename}' not found at {file_path}")

        with open(file_path, mode="r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def _atomic_write_csv(self, filename: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> Path:
        """Writes CSV dataset atomically via a temporary file."""
        target_path = self.silver_dir / filename
        temp_path = self.silver_dir / f"{filename}.tmp_{uuid.uuid4().hex[:8]}"

        try:
            with open(temp_path, mode="w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            temp_path.replace(target_path)
            return target_path
        except Exception as exc:
            if temp_path.exists():
                temp_path.unlink()
            self.logger.error("Failed writing Silver dataset %s: %s", target_path, exc)
            raise SilverWriteError(f"Failed writing Silver dataset to '{target_path}': {exc}") from exc

    def _quarantine(self, dataset: str, pk_col: str, pk_val: str, reason: str, details: str, raw_row: Dict[str, Any]):
        """Records a quarantined record and logs the incident."""
        q = QuarantinedRecord(
            dataset=dataset,
            primary_key_name=pk_col,
            primary_key_value=pk_val,
            reason=reason,
            details=details,
            raw_record=raw_row,
        )
        self.quarantined_records.append(q)
        self.logger.warning(
            "QUARANTINED record from %s [PK=%s:%s]: %s - %s",
            dataset, pk_col, pk_val, reason, details
        )

    # -------------------------------------------------------------------------
    # Dataset Transformation Handlers
    # -------------------------------------------------------------------------

    def transform_suppliers(self, raw_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Transforms suppliers dataset with deduplication, standardization, and tiering."""
        seen_pks: Dict[str, Dict[str, Any]] = {}
        transformed: List[Dict[str, Any]] = []

        for row in raw_rows:
            sup_id = self.clean_text(row.get("supplier_id")).upper()
            name = self.clean_text(row.get("supplier_name"))
            email = self.clean_text(row.get("contact_email")).lower()
            country = self.clean_text(row.get("country"))

            try:
                rel_score = round(float(row.get("reliability_score", 0.0)), 3)
                lead_time = int(row.get("lead_time_days", 0))
            except ValueError as exc:
                self._quarantine("suppliers.csv", "supplier_id", sup_id, "TYPE_ERROR", str(exc), row)
                continue

            # Derived Supplier Reliability Tier
            if rel_score >= 0.90:
                tier = "Tier 1 - Excellent"
            elif rel_score >= 0.80:
                tier = "Tier 2 - Good"
            elif rel_score >= 0.70:
                tier = "Tier 3 - Standard"
            else:
                tier = "Tier 4 - High Risk"

            record = {
                "supplier_id": sup_id,
                "supplier_name": name,
                "contact_email": email,
                "country": country,
                "reliability_score": f"{rel_score:.3f}",
                "lead_time_days": lead_time,
                "reliability_tier": tier,
            }

            # Deduplication: check PK collisions
            if sup_id in seen_pks:
                if seen_pks[sup_id] == record:
                    self.dedup_stats["suppliers.csv"] += 1
                    self.logger.info("Exact duplicate supplier '%s' deduplicated.", sup_id)
                else:
                    self._quarantine(
                        "suppliers.csv", "supplier_id", sup_id,
                        "CONFLICTING_PRIMARY_KEY",
                        f"Conflicting supplier record encountered for ID '{sup_id}'.",
                        row
                    )
                continue

            seen_pks[sup_id] = record
            transformed.append(record)

        return transformed

    def transform_warehouses(self, raw_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Transforms warehouses dataset with text standardization and capacity validation."""
        seen_pks: Dict[str, Dict[str, Any]] = {}
        transformed: List[Dict[str, Any]] = []

        for row in raw_rows:
            wh_id = self.clean_text(row.get("warehouse_id")).upper()
            name = self.clean_text(row.get("warehouse_name"))
            city = self.clean_text(row.get("city")).title()
            state = self.clean_text(row.get("state")).upper()
            country = self.clean_text(row.get("country"))

            try:
                capacity = int(row.get("capacity_units", 0))
                if capacity <= 0:
                    raise ValueError(f"Capacity must be positive, got {capacity}")
            except ValueError as exc:
                self._quarantine("warehouses.csv", "warehouse_id", wh_id, "VALUE_OUT_OF_BOUNDS", str(exc), row)
                continue

            record = {
                "warehouse_id": wh_id,
                "warehouse_name": name,
                "city": city,
                "state": state,
                "country": country,
                "capacity_units": capacity,
            }

            if wh_id in seen_pks:
                if seen_pks[wh_id] == record:
                    self.dedup_stats["warehouses.csv"] += 1
                else:
                    self._quarantine("warehouses.csv", "warehouse_id", wh_id, "CONFLICTING_PRIMARY_KEY", "Duplicate warehouse ID with conflicting attributes.", row)
                continue

            seen_pks[wh_id] = record
            transformed.append(record)

        return transformed

    def transform_products(self, raw_rows: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Transforms products catalog, calculating margin amounts and margin percentages."""
        seen_pks: Dict[str, Dict[str, Any]] = {}
        transformed: List[Dict[str, Any]] = []

        for row in raw_rows:
            p_id = self.clean_text(row.get("product_id")).upper()
            name = self.clean_text(row.get("product_name"))
            cat = self.clean_text(row.get("category")).title()
            subcat = self.clean_text(row.get("subcategory"))

            try:
                cost = round(float(row.get("unit_cost", 0.0)), 2)
                price = round(float(row.get("unit_price", 0.0)), 2)
                reorder_pt = int(row.get("reorder_point", 0))
                target_stk = int(row.get("target_stock_level", 0))

                if cost <= 0 or price <= 0:
                    raise ValueError(f"Cost ({cost}) and Price ({price}) must be positive.")
                if target_stk <= reorder_pt:
                    raise ValueError(f"Target stock ({target_stk}) must be > reorder point ({reorder_pt}).")
            except ValueError as exc:
                self._quarantine("products.csv", "product_id", p_id, "CONSTRAINT_VIOLATION", str(exc), row)
                continue

            # Derived Profit Margins
            margin_amt = round(price - cost, 2)
            margin_pct = round((margin_amt / price) * 100, 2) if price > 0 else 0.0

            record = {
                "product_id": p_id,
                "product_name": name,
                "category": cat,
                "subcategory": subcat,
                "unit_cost": f"{cost:.2f}",
                "unit_price": f"{price:.2f}",
                "margin_amount": f"{margin_amt:.2f}",
                "margin_percentage": f"{margin_pct:.2f}",
                "reorder_point": reorder_pt,
                "target_stock_level": target_stk,
            }

            if p_id in seen_pks:
                if seen_pks[p_id] == record:
                    self.dedup_stats["products.csv"] += 1
                else:
                    self._quarantine("products.csv", "product_id", p_id, "CONFLICTING_PRIMARY_KEY", "Conflicting product details for duplicate ID.", row)
                continue

            seen_pks[p_id] = record
            transformed.append(record)

        return transformed

    def transform_inventory(
        self,
        raw_rows: List[Dict[str, str]],
        valid_warehouses: Set[str],
        valid_products: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Transforms inventory records, asserting FK integrity and deriving inventory status."""
        seen_pks: Dict[str, Dict[str, Any]] = {}
        transformed: List[Dict[str, Any]] = []

        for row in raw_rows:
            inv_id = self.clean_text(row.get("inventory_id")).upper()
            wh_id = self.clean_text(row.get("warehouse_id")).upper()
            prd_id = self.clean_text(row.get("product_id")).upper()
            count_date = self.clean_text(row.get("last_counted_date"))

            # Referential integrity checks
            if wh_id not in valid_warehouses:
                self._quarantine("inventory.csv", "inventory_id", inv_id, "FOREIGN_KEY_VIOLATION", f"Referenced warehouse '{wh_id}' does not exist.", row)
                continue
            if prd_id not in valid_products:
                self._quarantine("inventory.csv", "inventory_id", inv_id, "FOREIGN_KEY_VIOLATION", f"Referenced product '{prd_id}' does not exist.", row)
                continue

            try:
                date_iso = self.parse_iso_date(count_date)
                soh = int(row.get("stock_on_hand", 0))
                alloc = int(row.get("stock_allocated", 0))
                res = int(row.get("stock_reserved", 0))

                if soh < 0 or alloc < 0 or res < 0:
                    raise ValueError(f"Negative quantities found: soh={soh}, alloc={alloc}, res={res}")
                if res > soh:
                    raise ValueError(f"Reserved stock ({res}) exceeds on-hand stock ({soh})")
            except ValueError as exc:
                self._quarantine("inventory.csv", "inventory_id", inv_id, "DATA_CORRUPTION", str(exc), row)
                continue

            # Derived available stock (units free for new orders)
            stock_avail = max(0, soh - alloc - res)

            # Derived Inventory Status
            reorder_pt = valid_products[prd_id]["reorder_point"]
            target_stk = valid_products[prd_id]["target_stock_level"]

            if soh == 0:
                inv_status = "OUT_OF_STOCK"
            elif soh <= reorder_pt:
                inv_status = "LOW_STOCK"
            elif soh > target_stk:
                inv_status = "OVERSTOCK"
            else:
                inv_status = "HEALTHY"

            record = {
                "inventory_id": inv_id,
                "warehouse_id": wh_id,
                "product_id": prd_id,
                "stock_on_hand": soh,
                "stock_allocated": alloc,
                "stock_reserved": res,
                "stock_available": stock_avail,
                "inventory_status": inv_status,
                "last_counted_date": date_iso,
            }

            if inv_id in seen_pks:
                if seen_pks[inv_id] == record:
                    self.dedup_stats["inventory.csv"] += 1
                else:
                    self._quarantine("inventory.csv", "inventory_id", inv_id, "CONFLICTING_PRIMARY_KEY", "Duplicate inventory ID with conflicting metrics.", row)
                continue

            seen_pks[inv_id] = record
            transformed.append(record)

        return transformed

    def transform_purchase_orders(
        self,
        raw_rows: List[Dict[str, str]],
        valid_suppliers: Set[str],
        valid_warehouses: Set[str],
    ) -> List[Dict[str, Any]]:
        """Transforms purchase orders, calculating delivery delays and enforcing lifecycle constraints."""
        seen_pks: Dict[str, Dict[str, Any]] = {}
        transformed: List[Dict[str, Any]] = []

        for row in raw_rows:
            order_id = self.clean_text(row.get("order_id")).upper()
            sup_id = self.clean_text(row.get("supplier_id")).upper()
            wh_id = self.clean_text(row.get("warehouse_id")).upper()
            status = self.clean_text(row.get("order_status")).upper()

            # Referential integrity checks
            if sup_id not in valid_suppliers:
                self._quarantine("purchase_orders.csv", "order_id", order_id, "FOREIGN_KEY_VIOLATION", f"Supplier '{sup_id}' does not exist.", row)
                continue
            if wh_id not in valid_warehouses:
                self._quarantine("purchase_orders.csv", "order_id", order_id, "FOREIGN_KEY_VIOLATION", f"Warehouse '{wh_id}' does not exist.", row)
                continue

            try:
                ord_date_iso = self.parse_iso_date(row.get("order_date"))
                exp_date_iso = self.parse_iso_date(row.get("expected_delivery_date"))
                act_date_raw = self.clean_text(row.get("actual_delivery_date"))
                act_date_iso = self.parse_iso_date(act_date_raw) if act_date_raw else ""
                total_amt = round(float(row.get("total_amount", 0.0)), 2)

                # Business Rule 7: Never treat pending or cancelled as received; never invent dates
                if status in ("PENDING", "CANCELLED") and act_date_iso != "":
                    raise ValueError(f"Order status '{status}' cannot have actual_delivery_date.")
                if status in ("RECEIVED", "PARTIALLY_RECEIVED") and not act_date_iso:
                    raise ValueError(f"Order status '{status}' must have actual_delivery_date.")

                # Timeline chronology
                ord_dt = datetime.strptime(ord_date_iso, "%Y-%m-%d").date()
                exp_dt = datetime.strptime(exp_date_iso, "%Y-%m-%d").date()

                if exp_dt < ord_dt:
                    raise ValueError(f"Expected delivery {exp_date_iso} precedes order date {ord_date_iso}")

                # Derived delivery metrics
                if act_date_iso:
                    act_dt = datetime.strptime(act_date_iso, "%Y-%m-%d").date()
                    if act_dt < ord_dt:
                        raise ValueError(f"Actual delivery {act_date_iso} precedes order date {ord_date_iso}")
                    delay_days = (act_dt - exp_dt).days
                    is_delayed = "TRUE" if delay_days > 0 else "FALSE"
                    lead_time_days = (act_dt - ord_dt).days
                else:
                    delay_days = ""
                    is_delayed = ""
                    lead_time_days = ""

            except ValueError as exc:
                self._quarantine("purchase_orders.csv", "order_id", order_id, "BUSINESS_RULE_VIOLATION", str(exc), row)
                continue

            record = {
                "order_id": order_id,
                "supplier_id": sup_id,
                "warehouse_id": wh_id,
                "order_date": ord_date_iso,
                "expected_delivery_date": exp_date_iso,
                "actual_delivery_date": act_date_iso,
                "order_status": status,
                "total_amount": f"{total_amt:.2f}",
                "delivery_delay_days": delay_days,
                "is_delayed": is_delayed,
                "fulfillment_lead_time_days": lead_time_days,
            }

            if order_id in seen_pks:
                if seen_pks[order_id] == record:
                    self.dedup_stats["purchase_orders.csv"] += 1
                else:
                    self._quarantine("purchase_orders.csv", "order_id", order_id, "CONFLICTING_PRIMARY_KEY", "Duplicate order ID with conflicting values.", row)
                continue

            seen_pks[order_id] = record
            transformed.append(record)

        return transformed

    def transform_purchase_order_items(
        self,
        raw_rows: List[Dict[str, str]],
        valid_orders: Dict[str, Dict[str, Any]],
        valid_products: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Transforms line items, computing pending quantities and fulfillment ratios."""
        seen_pks: Dict[str, Dict[str, Any]] = {}
        transformed: List[Dict[str, Any]] = []

        for row in raw_rows:
            item_id = self.clean_text(row.get("order_item_id")).upper()
            order_id = self.clean_text(row.get("order_id")).upper()
            prd_id = self.clean_text(row.get("product_id")).upper()

            # Referential integrity
            if order_id not in valid_orders:
                self._quarantine("purchase_order_items.csv", "order_item_id", item_id, "FOREIGN_KEY_VIOLATION", f"Parent order '{order_id}' does not exist.", row)
                continue
            if prd_id not in valid_products:
                self._quarantine("purchase_order_items.csv", "order_item_id", item_id, "FOREIGN_KEY_VIOLATION", f"Product '{prd_id}' does not exist.", row)
                continue

            try:
                qty_ord = int(row.get("quantity_ordered", 0))
                qty_rec = int(row.get("quantity_received", 0))
                price = round(float(row.get("unit_price", 0.0)), 2)

                if qty_ord <= 0 or qty_rec < 0 or qty_rec > qty_ord:
                    raise ValueError(f"Invalid quantities: ordered={qty_ord}, received={qty_rec}")

                parent_status = valid_orders[order_id]["order_status"]
                if parent_status in ("PENDING", "CANCELLED") and qty_rec > 0:
                    raise ValueError(f"Order is {parent_status} but line item has quantity_received={qty_rec}")

                line_tot = round(qty_ord * price, 2)
                qty_pending = max(0, qty_ord - qty_rec)
                fulfillment_ratio = round(qty_rec / qty_ord, 4) if qty_ord > 0 else 0.0

            except ValueError as exc:
                self._quarantine("purchase_order_items.csv", "order_item_id", item_id, "QUANTITY_MATH_VIOLATION", str(exc), row)
                continue

            record = {
                "order_item_id": item_id,
                "order_id": order_id,
                "product_id": prd_id,
                "quantity_ordered": qty_ord,
                "quantity_received": qty_rec,
                "quantity_pending": qty_pending,
                "fulfillment_ratio": f"{fulfillment_ratio:.4f}",
                "unit_price": f"{price:.2f}",
                "line_total": f"{line_tot:.2f}",
            }

            if item_id in seen_pks:
                if seen_pks[item_id] == record:
                    self.dedup_stats["purchase_order_items.csv"] += 1
                else:
                    self._quarantine("purchase_order_items.csv", "order_item_id", item_id, "CONFLICTING_PRIMARY_KEY", "Duplicate item ID with conflicting values.", row)
                continue

            seen_pks[item_id] = record
            transformed.append(record)

        return transformed

    # -------------------------------------------------------------------------
    # Pipeline Orchestration
    # -------------------------------------------------------------------------

    def validate_bronze_source(self) -> None:
        """Audits Bronze inputs with DataQualityValidator, halting if critical checks fail."""
        self.logger.info("Executing Bronze data quality pre-validation gate...")
        validator = DataQualityValidator(data_dir=self.bronze_dir)
        report = validator.validate_all()

        if report.overall_status == "FAILED" or report.critical_errors > 0:
            error_msg = (
                f"Bronze validation GATE FAILED: {report.critical_errors} critical errors "
                f"across {report.failed_checks} failed checks. Halting Silver transformation."
            )
            self.logger.error(error_msg)
            raise ValidationHaltError(error_msg)

        self.logger.info(
            "Bronze validation GATE PASSED: %d checks passed (%d business warnings).",
            report.passed_checks, report.warning_checks
        )

    def write_quarantine_manifest(self) -> List[str]:
        """Writes quarantined records by dataset to JSON for operational inspection."""
        written_files = []
        records_by_ds: Dict[str, List[Dict[str, Any]]] = {}

        for q in self.quarantined_records:
            records_by_ds.setdefault(q.dataset, []).append(asdict(q))

        for ds_name, records in records_by_ds.items():
            out_name = f"{Path(ds_name).stem}_quarantine.json"
            target_file = self.quarantine_dir / out_name
            with open(target_file, mode="w", encoding="utf-8") as f:
                json.dump({"dataset": ds_name, "count": len(records), "records": records}, f, indent=2)
            written_files.append(str(target_file.resolve()))

        return written_files

    def write_report(self, batch_id: str, ds_stats: Dict[str, Dict[str, Any]]) -> Path:
        """Writes detailed machine-readable Silver transformation audit report."""
        report_path = self.silver_dir / self.report_filename
        temp_path = self.silver_dir / f"{self.report_filename}.tmp_{batch_id}"

        total_in = sum(s["input_records"] for s in ds_stats.values())
        total_out = sum(s["output_records"] for s in ds_stats.values())
        total_quarantined = len(self.quarantined_records)
        total_dedup = sum(self.dedup_stats.values())

        report_payload = {
            "batch_id": batch_id,
            "transformation_timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "source_directory": str(self.bronze_dir.resolve()),
            "silver_directory": str(self.silver_dir.resolve()),
            "overall_status": "SUCCESS",
            "summary": {
                "total_input_records": total_in,
                "total_output_records": total_out,
                "total_quarantined_records": total_quarantined,
                "total_deduplicated_records": total_dedup,
            },
            "applied_transformations": [
                "Whitespace stripping and casing normalization (title/upper/lower)",
                "Numeric rounding and formatting (2 decimals for currency, 3 for reliability, 4 for fulfillment)",
                "Strict ISO 8601 calendar date parsing and standardization",
                "Referential integrity preservation across parent-child datasets",
                "Deterministic deduplication and quarantine routing for corrupted records",
                "Derived fields: profit margins, inventory stock availability and statuses, delivery delay metrics",
            ],
            "datasets": ds_stats,
            "quarantine_summary": {
                "total_quarantined": total_quarantined,
                "quarantine_directory": str(self.quarantine_dir.resolve()),
            },
        }

        with open(temp_path, mode="w", encoding="utf-8") as f:
            json.dump(report_payload, f, indent=2)
        temp_path.replace(report_path)
        return report_path

    def transform_all(self, validate_first: bool = True) -> Dict[str, Any]:
        """Executes complete Silver transformations across all six datasets.

        Args:
            validate_first: Whether to execute Bronze quality validation first.

        Returns:
            Dictionary summary of the transformation run.

        Raises:
            ValidationHaltError: If validate_first is True and Bronze validation fails.
            SilverWriteError: If writing to Silver fails.
        """
        batch_id = str(uuid.uuid4())
        self.logger.info("Starting Silver transformation batch %s", batch_id)

        # 1. Validation gate check
        if validate_first:
            self.validate_bronze_source()

        # Reset batch states
        self.quarantined_records = []
        self.dedup_stats = {ds: 0 for ds in EXPECTED_DATASETS}

        # 2. Read Bronze inputs
        raw_inputs = {
            "suppliers.csv": self._read_bronze_csv("suppliers.csv"),
            "warehouses.csv": self._read_bronze_csv("warehouses.csv"),
            "products.csv": self._read_bronze_csv("products.csv"),
            "inventory.csv": self._read_bronze_csv("inventory.csv"),
            "purchase_orders.csv": self._read_bronze_csv("purchase_orders.csv"),
            "purchase_order_items.csv": self._read_bronze_csv("purchase_order_items.csv"),
        }

        # 3. Transform entity datasets in topological order
        silver_suppliers = self.transform_suppliers(raw_inputs["suppliers.csv"])
        valid_suppliers_set = {s["supplier_id"] for s in silver_suppliers}

        silver_warehouses = self.transform_warehouses(raw_inputs["warehouses.csv"])
        valid_warehouses_set = {w["warehouse_id"] for w in silver_warehouses}

        silver_products = self.transform_products(raw_inputs["products.csv"])
        valid_products_map = {p["product_id"]: p for p in silver_products}

        silver_inventory = self.transform_inventory(raw_inputs["inventory.csv"], valid_warehouses_set, valid_products_map)

        silver_pos = self.transform_purchase_orders(raw_inputs["purchase_orders.csv"], valid_suppliers_set, valid_warehouses_set)
        valid_pos_map = {o["order_id"]: o for o in silver_pos}

        silver_poi = self.transform_purchase_order_items(raw_inputs["purchase_order_items.csv"], valid_pos_map, valid_products_map)

        # 4. Atomic write of conformed Silver CSV files
        datasets_to_write = [
            ("suppliers.csv", silver_suppliers),
            ("warehouses.csv", silver_warehouses),
            ("products.csv", silver_products),
            ("inventory.csv", silver_inventory),
            ("purchase_orders.csv", silver_pos),
            ("purchase_order_items.csv", silver_poi),
        ]

        ds_stats = {}
        for fname, rows in datasets_to_write:
            if rows:
                cols = list(rows[0].keys())
                self._atomic_write_csv(fname, rows, cols)
            else:
                cols = []

            in_count = len(raw_inputs[fname])
            ds_stats[fname] = {
                "input_records": in_count,
                "output_records": len(rows),
                "quarantined_records": sum(1 for q in self.quarantined_records if q.dataset == fname),
                "deduplicated_records": self.dedup_stats[fname],
                "columns": cols,
            }

        # 5. Write quarantine files & report
        self.write_quarantine_manifest()
        report_path = self.write_report(batch_id, ds_stats)

        total_in = sum(s["input_records"] for s in ds_stats.values())
        total_out = sum(s["output_records"] for s in ds_stats.values())

        summary = {
            "batch_id": batch_id,
            "status": "SUCCESS",
            "report_path": str(report_path.resolve()),
            "total_input_records": total_in,
            "total_output_records": total_out,
            "total_quarantined": len(self.quarantined_records),
            "total_deduplicated": sum(self.dedup_stats.values()),
        }

        self.logger.info(
            "Silver transformation batch %s complete: %d in -> %d out (%d quarantined, %d deduplicated).",
            batch_id, total_in, total_out, summary["total_quarantined"], summary["total_deduplicated"]
        )

        return summary


def main():
    """CLI entrypoint to run Bronze quality validation followed by Silver transformation."""
    parser = argparse.ArgumentParser(description="RetailFlow Silver Layer Transformations")
    parser.add_argument(
        "--bronze-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "bronze"),
        help="Path to Bronze directory (default: data/bronze)",
    )
    parser.add_argument(
        "--silver-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "silver"),
        help="Path to Silver destination directory (default: data/silver)",
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip upstream Bronze quality validation check",
    )

    args = parser.parse_args()

    print("=" * 65)
    print("RETAILFLOW SILVER LAYER TRANSFORMATION PIPELINE")
    print("=" * 65)
    print(f"  Bronze Source : {args.bronze_dir}")
    print(f"  Silver Output : {args.silver_dir}")
    print(f"  Validate First: {not args.no_validate}")
    print("-" * 65)

    transformer = SilverDataTransformer(
        bronze_dir=args.bronze_dir,
        silver_dir=args.silver_dir,
    )

    try:
        summary = transformer.transform_all(validate_first=not args.no_validate)
    except ValidationHaltError as val_err:
        print("\n[CRITICAL ERROR] Upstream Data Quality Gate FAILED!")
        print(f"Details: {val_err}")
        print("\nSilver transformations halted. Downstream promotion blocked.")
        sys.exit(1)
    except Exception as exc:
        print(f"\n[CRITICAL ERROR] Silver transformation failed: {exc}")
        sys.exit(1)

    print("\nSilver Transformation Succeeded!")
    print(f"  Batch ID           : {summary['batch_id']}")
    print(f"  Total Input Rows   : {summary['total_input_records']}")
    print(f"  Total Silver Rows  : {summary['total_output_records']}")
    print(f"  Quarantined Rows   : {summary['total_quarantined']}")
    print(f"  Deduplicated Rows  : {summary['total_deduplicated']}")
    print(f"  Audit Report Path  : {summary['report_path']}")
    print("=" * 65)

    sys.exit(0)


if __name__ == "__main__":
    main()
