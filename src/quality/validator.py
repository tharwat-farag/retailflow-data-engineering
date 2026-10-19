"""Data Quality Validator and Business Contract Enforcer for RetailFlow.

Performs schema validation, primary and foreign key verification,
and supply chain business rule assertions across Bronze datasets.
Produces a machine-readable JSON report and distinguishes critical
errors from business warnings.
"""

import argparse
import csv
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# Define PROJECT_ROOT and configure sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.quality.contracts import (
    ALL_CONTRACTS,
    ColumnContract,
    TableContract,
)
from src.utils.logger import get_logger


class CheckSeverity(str, Enum):
    """Severity classification for data quality checks."""
    ERROR = "ERROR"        # Critical violation: schema break, orphan key, negative qty
    WARNING = "WARNING"    # Advisory business scenario: delayed shipment, stockout


class CheckStatus(str, Enum):
    """Outcome status of an individual check."""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"


@dataclass
class QualityCheckResult:
    """Detailed outcome of a single validation check.

    Attributes:
        check_id: Unique identifier for the check (e.g. 'inventory_no_negative_stock').
        dataset: Dataset filename evaluated.
        check_name: Human-readable check title.
        severity: Severity level (ERROR or WARNING).
        status: PASSED, FAILED, or WARNING.
        total_records: Number of records evaluated.
        failed_records: Number of records failing the check.
        details: Actionable summary description of the outcome.
        sample_issues: Sanitized sample of failing records (max 5) for debugging.
    """
    check_id: str
    dataset: str
    check_name: str
    severity: CheckSeverity
    status: CheckStatus
    total_records: int
    failed_records: int
    details: str
    sample_issues: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Summary and details of the complete data quality evaluation."""
    validation_timestamp_utc: str
    data_directory: str
    overall_status: str
    total_checks: int
    passed_checks: int
    warning_checks: int
    failed_checks: int
    critical_errors: int
    warnings: int
    metrics: Dict[str, Any]
    checks: List[QualityCheckResult]

    def to_dict(self) -> Dict[str, Any]:
        """Converts report to serializable dictionary."""
        return {
            "validation_timestamp_utc": self.validation_timestamp_utc,
            "data_directory": self.data_directory,
            "overall_status": self.overall_status,
            "summary": {
                "total_checks": self.total_checks,
                "passed_checks": self.passed_checks,
                "warning_checks": self.warning_checks,
                "failed_checks": self.failed_checks,
                "critical_errors": self.critical_errors,
                "warnings": self.warnings,
            },
            "metrics": self.metrics,
            "checks": [asdict(c) for c in self.checks],
        }


class DataQualityValidator:
    """Comprehensive data quality and business rule engine for RetailFlow datasets."""

    def __init__(
        self,
        data_dir: Optional[Union[str, Path]] = None,
        contracts: Optional[Dict[str, TableContract]] = None,
    ):
        """Initializes the validator with target data directory.

        Args:
            data_dir: Path to directory containing CSV datasets (e.g. data/bronze).
            contracts: Optional custom dictionary of TableContracts.
        """
        self.logger = get_logger("retailflow.quality")
        default_dir = PROJECT_ROOT / "data" / "bronze"

        if data_dir is None:
            self.data_dir = default_dir
        else:
            p = Path(data_dir)
            self.data_dir = p if p.is_absolute() else (PROJECT_ROOT / p).resolve()

        self.contracts = contracts or ALL_CONTRACTS
        self.checks: List[QualityCheckResult] = []
        self.metrics: Dict[str, Any] = {}

    def _read_csv(self, filename: str) -> Tuple[List[str], List[Dict[str, str]]]:
        """Reads CSV file in read-only mode.

        Args:
            filename: CSV filename.

        Returns:
            Tuple of (header_list, list_of_row_dicts).
        """
        file_path = self.data_dir / filename
        if not file_path.exists() or not file_path.is_file():
            return [], []

        with open(file_path, mode="r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                return [], []
            f.seek(0)
            dict_reader = csv.DictReader(f)
            rows = list(dict_reader)
            return header, rows

    def _add_check(
        self,
        check_id: str,
        dataset: str,
        check_name: str,
        severity: CheckSeverity,
        total_records: int,
        failed_records: int,
        details: str,
        sample_issues: Optional[List[Dict[str, Any]]] = None,
    ):
        """Helper to append a structured check result."""
        if failed_records == 0:
            status = CheckStatus.PASSED
        else:
            status = CheckStatus.FAILED if severity == CheckSeverity.ERROR else CheckStatus.WARNING

        self.checks.append(
            QualityCheckResult(
                check_id=check_id,
                dataset=dataset,
                check_name=check_name,
                severity=severity,
                status=status,
                total_records=total_records,
                failed_records=failed_records,
                details=details,
                sample_issues=(sample_issues or [])[:5],
            )
        )

    def validate_file_presence_and_structure(self, filename: str, contract: TableContract) -> Optional[List[Dict[str, str]]]:
        """Validates that a dataset exists, is non-empty, and has all required columns."""
        file_path = self.data_dir / filename

        # 1. Existence check
        if not file_path.exists() or not file_path.is_file():
            self._add_check(
                check_id=f"{filename}_file_presence",
                dataset=filename,
                check_name="File Presence Check",
                severity=CheckSeverity.ERROR,
                total_records=0,
                failed_records=1,
                details=f"Required dataset file '{filename}' was not found in {self.data_dir}.",
            )
            return None

        # 2. Non-empty check
        if file_path.stat().st_size == 0:
            self._add_check(
                check_id=f"{filename}_file_non_empty",
                dataset=filename,
                check_name="Non-Empty File Check",
                severity=CheckSeverity.ERROR,
                total_records=0,
                failed_records=1,
                details=f"Dataset file '{filename}' is completely empty (0 bytes).",
            )
            return None

        header, rows = self._read_csv(filename)

        if not rows:
            self._add_check(
                check_id=f"{filename}_records_exist",
                dataset=filename,
                check_name="Data Rows Existence Check",
                severity=CheckSeverity.ERROR,
                total_records=0,
                failed_records=1,
                details=f"Dataset file '{filename}' contains only a header row with 0 data records.",
            )
            return None

        self._add_check(
            check_id=f"{filename}_file_presence",
            dataset=filename,
            check_name="File Presence Check",
            severity=CheckSeverity.ERROR,
            total_records=len(rows),
            failed_records=0,
            details=f"Dataset '{filename}' is present with {len(rows)} records.",
        )

        # 3. Required columns presence
        missing_cols = [col for col, c_spec in contract.columns.items() if c_spec.required and col not in header]
        if missing_cols:
            self._add_check(
                check_id=f"{filename}_required_columns",
                dataset=filename,
                check_name="Required Columns Check",
                severity=CheckSeverity.ERROR,
                total_records=len(rows),
                failed_records=len(missing_cols),
                details=f"Missing required columns in '{filename}': {', '.join(missing_cols)}.",
                sample_issues=[{"missing_columns": missing_cols}],
            )
        else:
            self._add_check(
                check_id=f"{filename}_required_columns",
                dataset=filename,
                check_name="Required Columns Check",
                severity=CheckSeverity.ERROR,
                total_records=len(rows),
                failed_records=0,
                details=f"All {len(contract.columns)} required columns are present in '{filename}'.",
            )

        return rows

    def validate_column_contracts(self, filename: str, contract: TableContract, rows: List[Dict[str, str]]):
        """Validates nullability, data types, and value constraints for every column."""
        for col_name, col_spec in contract.columns.items():
            missing_vals = []
            type_errors = []
            constraint_errors = []

            for idx, r in enumerate(rows, start=2):
                val = r.get(col_name)

                # Nullability check
                if val is None or val.strip() == "":
                    if not col_spec.nullable:
                        missing_vals.append({
                            "line": idx,
                            "pk": r.get(contract.primary_key, "UNKNOWN"),
                            "issue": f"Missing value in non-nullable column '{col_name}'",
                        })
                    continue

                # Type & constraint checks
                val_clean = val.strip()

                if col_spec.data_type == "int":
                    try:
                        int_val = int(val_clean)
                        if col_spec.min_value is not None and int_val < col_spec.min_value:
                            constraint_errors.append({
                                "line": idx,
                                "pk": r.get(contract.primary_key, "UNKNOWN"),
                                "issue": f"Value {int_val} < minimum {col_spec.min_value}",
                            })
                        if col_spec.max_value is not None and int_val > col_spec.max_value:
                            constraint_errors.append({
                                "line": idx,
                                "pk": r.get(contract.primary_key, "UNKNOWN"),
                                "issue": f"Value {int_val} > maximum {col_spec.max_value}",
                            })
                    except ValueError:
                        type_errors.append({
                            "line": idx,
                            "pk": r.get(contract.primary_key, "UNKNOWN"),
                            "issue": f"Cannot parse '{val_clean}' as integer",
                        })

                elif col_spec.data_type == "float":
                    try:
                        flt_val = float(val_clean)
                        if col_spec.min_value is not None and flt_val < col_spec.min_value:
                            constraint_errors.append({
                                "line": idx,
                                "pk": r.get(contract.primary_key, "UNKNOWN"),
                                "issue": f"Value {flt_val} < minimum {col_spec.min_value}",
                            })
                        if col_spec.max_value is not None and flt_val > col_spec.max_value:
                            constraint_errors.append({
                                "line": idx,
                                "pk": r.get(contract.primary_key, "UNKNOWN"),
                                "issue": f"Value {flt_val} > maximum {col_spec.max_value}",
                            })
                    except ValueError:
                        type_errors.append({
                            "line": idx,
                            "pk": r.get(contract.primary_key, "UNKNOWN"),
                            "issue": f"Cannot parse '{val_clean}' as float",
                        })

                elif col_spec.data_type == "date":
                    fmt = col_spec.date_format or "%Y-%m-%d"
                    try:
                        datetime.strptime(val_clean, fmt)
                    except ValueError:
                        type_errors.append({
                            "line": idx,
                            "pk": r.get(contract.primary_key, "UNKNOWN"),
                            "issue": f"Date '{val_clean}' does not match format {fmt}",
                        })

                # Categorical allowed values check
                if col_spec.allowed_values and val_clean not in col_spec.allowed_values:
                    constraint_errors.append({
                        "line": idx,
                        "pk": r.get(contract.primary_key, "UNKNOWN"),
                        "issue": f"Value '{val_clean}' not in allowed set: {sorted(col_spec.allowed_values)}",
                    })

            # Record check results
            total_r = len(rows)
            # Nullability
            self._add_check(
                check_id=f"{filename}_{col_name}_nullability",
                dataset=filename,
                check_name=f"Nullability Check ({col_name})",
                severity=CheckSeverity.ERROR,
                total_records=total_r,
                failed_records=len(missing_vals),
                details=f"Column '{col_name}' has {len(missing_vals)} disallowed null/empty entries.",
                sample_issues=missing_vals,
            )

            # Data types
            self._add_check(
                check_id=f"{filename}_{col_name}_type",
                dataset=filename,
                check_name=f"Data Type Check ({col_name})",
                severity=CheckSeverity.ERROR,
                total_records=total_r,
                failed_records=len(type_errors),
                details=f"Column '{col_name}' has {len(type_errors)} type conversion failures.",
                sample_issues=type_errors,
            )

            # Constraints (ranges/enums)
            if col_spec.min_value is not None or col_spec.max_value is not None or col_spec.allowed_values:
                self._add_check(
                    check_id=f"{filename}_{col_name}_constraints",
                    dataset=filename,
                    check_name=f"Value Constraints Check ({col_name})",
                    severity=CheckSeverity.ERROR,
                    total_records=total_r,
                    failed_records=len(constraint_errors),
                    details=f"Column '{col_name}' has {len(constraint_errors)} value range or enum violations.",
                    sample_issues=constraint_errors,
                )

    def validate_keys(self, filename: str, contract: TableContract, rows: List[Dict[str, str]]):
        """Validates primary key and composite key uniqueness."""
        pk_col = contract.primary_key
        seen_pks = set()
        dup_pks = []

        for idx, r in enumerate(rows, start=2):
            pk_val = r.get(pk_col)
            if pk_val in seen_pks:
                dup_pks.append({"line": idx, "duplicate_pk": pk_val})
            else:
                seen_pks.add(pk_val)

        self._add_check(
            check_id=f"{filename}_primary_key_uniqueness",
            dataset=filename,
            check_name=f"Primary Key Uniqueness ({pk_col})",
            severity=CheckSeverity.ERROR,
            total_records=len(rows),
            failed_records=len(dup_pks),
            details=f"Primary key '{pk_col}' in '{filename}' has {len(dup_pks)} duplicates.",
            sample_issues=dup_pks,
        )

        # Composite keys
        for comp_cols in contract.composite_keys:
            comp_name = "+".join(comp_cols)
            seen_comp = set()
            dup_comp = []

            for idx, r in enumerate(rows, start=2):
                c_val = tuple(r.get(c) for c in comp_cols)
                if c_val in seen_comp:
                    dup_comp.append({"line": idx, "duplicate_composite_key": dict(zip(comp_cols, c_val))})
                else:
                    seen_comp.add(c_val)

            self._add_check(
                check_id=f"{filename}_composite_key_{comp_name}",
                dataset=filename,
                check_name=f"Composite Key Uniqueness ({comp_name})",
                severity=CheckSeverity.ERROR,
                total_records=len(rows),
                failed_records=len(dup_comp),
                details=f"Composite key ({comp_name}) in '{filename}' has {len(dup_comp)} duplicates.",
                sample_issues=dup_comp,
            )

    def validate_referential_integrity(self, datasets: Dict[str, List[Dict[str, str]]]):
        """Validates foreign key relationships across all loaded datasets."""
        for filename, contract in self.contracts.items():
            if filename not in datasets or not datasets[filename]:
                continue

            child_rows = datasets[filename]

            for fk in contract.foreign_keys:
                parent_dataset = fk.parent_dataset
                parent_col = fk.parent_column
                child_col = fk.column

                if parent_dataset not in datasets or not datasets[parent_dataset]:
                    self._add_check(
                        check_id=f"{filename}_{child_col}_parent_availability",
                        dataset=filename,
                        check_name=f"Foreign Key Parent Availability ({child_col})",
                        severity=CheckSeverity.ERROR,
                        total_records=len(child_rows),
                        failed_records=len(child_rows),
                        details=f"Referenced parent dataset '{parent_dataset}' is missing or empty.",
                    )
                    continue

                parent_keys = {r.get(parent_col) for r in datasets[parent_dataset] if r.get(parent_col)}
                orphans = []

                for idx, r in enumerate(child_rows, start=2):
                    fk_val = r.get(child_col)
                    if fk_val and fk_val not in parent_keys:
                        orphans.append({
                            "line": idx,
                            "pk": r.get(contract.primary_key, "UNKNOWN"),
                            "fk_column": child_col,
                            "missing_parent_id": fk_val,
                        })

                self._add_check(
                    check_id=f"{filename}_{child_col}_foreign_key",
                    dataset=filename,
                    check_name=f"Foreign Key Integrity ({child_col} -> {parent_dataset}.{parent_col})",
                    severity=CheckSeverity.ERROR,
                    total_records=len(child_rows),
                    failed_records=len(orphans),
                    details=f"Foreign key '{child_col}' has {len(orphans)} orphan records.",
                    sample_issues=orphans,
                )

    def validate_business_rules(self, datasets: Dict[str, List[Dict[str, str]]]):
        """Enforces domain-specific retail supply chain business rules."""
        # 1. Inventory Business Rules
        if "inventory.csv" in datasets and datasets["inventory.csv"]:
            inv_rows = datasets["inventory.csv"]
            neg_stock = []
            reserved_exceeds_soh = []
            stockouts = []
            low_stock = []

            # Products lookup for reorder points
            prod_lookup = {}
            if "products.csv" in datasets and datasets["products.csv"]:
                prod_lookup = {r["product_id"]: int(r["reorder_point"]) for r in datasets["products.csv"] if r.get("reorder_point")}

            for idx, r in enumerate(inv_rows, start=2):
                try:
                    soh = int(r.get("stock_on_hand", 0))
                    alloc = int(r.get("stock_allocated", 0))
                    res = int(r.get("stock_reserved", 0))

                    # Non-negative quantities (ERROR)
                    if soh < 0 or alloc < 0 or res < 0:
                        neg_stock.append({"line": idx, "inv_id": r.get("inventory_id"), "issue": f"Negative quantities: soh={soh}, alloc={alloc}, res={res}"})

                    # Quantity reserved cannot exceed on-hand (ERROR)
                    if res > soh:
                        reserved_exceeds_soh.append({"line": idx, "inv_id": r.get("inventory_id"), "issue": f"Reserved {res} > On-Hand {soh}"})

                    # Business Metric / Warning: Stockouts
                    if soh == 0:
                        stockouts.append({"inv_id": r.get("inventory_id"), "product_id": r.get("product_id"), "warehouse_id": r.get("warehouse_id")})

                    # Business Metric / Warning: Low Stock below reorder point
                    p_id = r.get("product_id")
                    if p_id in prod_lookup and 0 < soh <= prod_lookup[p_id]:
                        low_stock.append({"inv_id": r.get("inventory_id"), "product_id": p_id, "stock_on_hand": soh, "reorder_point": prod_lookup[p_id]})

                except ValueError:
                    pass

            self._add_check(
                check_id="inventory_non_negative_quantities",
                dataset="inventory.csv",
                check_name="Non-Negative Inventory Quantities",
                severity=CheckSeverity.ERROR,
                total_records=len(inv_rows),
                failed_records=len(neg_stock),
                details=f"Found {len(neg_stock)} records with negative stock, allocation, or reserves.",
                sample_issues=neg_stock,
            )

            self._add_check(
                check_id="inventory_reserved_le_on_hand",
                dataset="inventory.csv",
                check_name="Reserved Stock <= On Hand",
                severity=CheckSeverity.ERROR,
                total_records=len(inv_rows),
                failed_records=len(reserved_exceeds_soh),
                details=f"Found {len(reserved_exceeds_soh)} records where reserved units exceed physical on-hand stock.",
                sample_issues=reserved_exceeds_soh,
            )

            # Warning checks for Stockouts & Low Stock
            self._add_check(
                check_id="inventory_stockout_alerts",
                dataset="inventory.csv",
                check_name="Inventory Stockout Alerts",
                severity=CheckSeverity.WARNING,
                total_records=len(inv_rows),
                failed_records=len(stockouts),
                details=f"{len(stockouts)} warehouse inventory items are currently out of stock (stock_on_hand = 0).",
                sample_issues=stockouts,
            )
            self.metrics["total_stockouts"] = len(stockouts)

            self._add_check(
                check_id="inventory_low_stock_triggers",
                dataset="inventory.csv",
                check_name="Low Stock Reorder Triggers",
                severity=CheckSeverity.WARNING,
                total_records=len(inv_rows),
                failed_records=len(low_stock),
                details=f"{len(low_stock)} inventory items are at or below reorder threshold.",
                sample_issues=low_stock,
            )
            self.metrics["total_low_stock_alerts"] = len(low_stock)

        # 2. Purchase Orders & Items Business Rules
        if "purchase_orders.csv" in datasets and datasets["purchase_orders.csv"]:
            po_rows = datasets["purchase_orders.csv"]
            poi_rows = datasets.get("purchase_order_items.csv", [])

            # Items by order lookup
            items_by_order: Dict[str, List[Dict[str, str]]] = {}
            for item in poi_rows:
                items_by_order.setdefault(item.get("order_id", ""), []).append(item)

            missing_delivery_date = []
            cancelled_pending_with_actual_date = []
            cancelled_pending_with_received_qty = []
            delayed_orders = []
            delivery_precedes_order = []

            for idx, r in enumerate(po_rows, start=2):
                oid = r.get("order_id", "")
                status = r.get("order_status", "")
                act_date_str = r.get("actual_delivery_date", "").strip()
                exp_date_str = r.get("expected_delivery_date", "").strip()
                ord_date_str = r.get("order_date", "").strip()

                # Rule: Received orders must have actual delivery dates (ERROR)
                if status in ("RECEIVED", "PARTIALLY_RECEIVED"):
                    if not act_date_str:
                        missing_delivery_date.append({
                            "line": idx, "order_id": oid, "status": status,
                            "issue": "Order marked received but actual_delivery_date is missing"
                        })
                # Rule: Cancelled and pending orders must not be treated as received (ERROR)
                elif status in ("PENDING", "CANCELLED"):
                    if act_date_str:
                        cancelled_pending_with_actual_date.append({
                            "line": idx, "order_id": oid, "status": status,
                            "issue": f"Order is {status} but has actual_delivery_date '{act_date_str}'"
                        })
                    for item in items_by_order.get(oid, []):
                        try:
                            if int(item.get("quantity_received", 0)) > 0:
                                cancelled_pending_with_received_qty.append({
                                    "order_id": oid, "order_item_id": item.get("order_item_id"),
                                    "issue": f"Order is {status} but line item has quantity_received > 0"
                                })
                        except ValueError:
                            pass

                # Delivery date timeline evaluations
                if act_date_str and ord_date_str:
                    try:
                        act_dt = datetime.strptime(act_date_str, "%Y-%m-%d").date()
                        ord_dt = datetime.strptime(ord_date_str, "%Y-%m-%d").date()
                        exp_dt = datetime.strptime(exp_date_str, "%Y-%m-%d").date()

                        # Error: actual delivery date before order date
                        if act_dt < ord_dt:
                            delivery_precedes_order.append({
                                "line": idx, "order_id": oid,
                                "issue": f"Actual delivery {act_date_str} is before order date {ord_date_str}"
                            })

                        # Warning/Metric: Supplier delay evaluation
                        if act_dt > exp_dt:
                            delay_days = (act_dt - exp_dt).days
                            delayed_orders.append({
                                "order_id": oid, "supplier_id": r.get("supplier_id"),
                                "delay_days": delay_days,
                                "expected": exp_date_str, "actual": act_date_str
                            })
                    except ValueError:
                        pass

            self._add_check(
                check_id="po_received_has_actual_delivery_date",
                dataset="purchase_orders.csv",
                check_name="Received Orders Delivery Date Presence",
                severity=CheckSeverity.ERROR,
                total_records=len(po_rows),
                failed_records=len(missing_delivery_date),
                details=f"Found {len(missing_delivery_date)} received/partially received orders missing actual delivery dates.",
                sample_issues=missing_delivery_date,
            )

            self._add_check(
                check_id="po_pending_cancelled_not_received_date",
                dataset="purchase_orders.csv",
                check_name="Pending/Cancelled Delivery Date Absence",
                severity=CheckSeverity.ERROR,
                total_records=len(po_rows),
                failed_records=len(cancelled_pending_with_actual_date),
                details=f"Found {len(cancelled_pending_with_actual_date)} pending or cancelled orders with delivery dates.",
                sample_issues=cancelled_pending_with_actual_date,
            )

            self._add_check(
                check_id="po_pending_cancelled_zero_received_qty",
                dataset="purchase_order_items.csv",
                check_name="Pending/Cancelled Zero Quantity Received",
                severity=CheckSeverity.ERROR,
                total_records=len(poi_rows),
                failed_records=len(cancelled_pending_with_received_qty),
                details=f"Found {len(cancelled_pending_with_received_qty)} items with received quantities on pending/cancelled orders.",
                sample_issues=cancelled_pending_with_received_qty,
            )

            self._add_check(
                check_id="po_actual_delivery_after_order_date",
                dataset="purchase_orders.csv",
                check_name="Delivery Date Chronology Check",
                severity=CheckSeverity.ERROR,
                total_records=len(po_rows),
                failed_records=len(delivery_precedes_order),
                details=f"Found {len(delivery_precedes_order)} orders where delivery date precedes order placement date.",
                sample_issues=delivery_precedes_order,
            )

            # Warning / Supplier Performance Metric
            avg_delay = round(sum(d["delay_days"] for d in delayed_orders) / max(1, len(delayed_orders)), 1)
            self._add_check(
                check_id="po_supplier_delayed_deliveries",
                dataset="purchase_orders.csv",
                check_name="Supplier Late Delivery Analysis",
                severity=CheckSeverity.WARNING,
                total_records=len(po_rows),
                failed_records=len(delayed_orders),
                details=f"{len(delayed_orders)} orders arrived past expected delivery date (avg delay: {avg_delay} days).",
                sample_issues=delayed_orders,
            )
            self.metrics["total_delayed_orders"] = len(delayed_orders)
            self.metrics["average_supplier_delay_days"] = avg_delay

        # 3. PO Items Specific Quantities and Pricing Math (ERROR)
        if "purchase_order_items.csv" in datasets and datasets["purchase_order_items.csv"]:
            poi_rows = datasets["purchase_order_items.csv"]
            invalid_qty = []
            math_mismatch = []

            for idx, r in enumerate(poi_rows, start=2):
                try:
                    qty_ord = int(r.get("quantity_ordered", 0))
                    qty_rec = int(r.get("quantity_received", 0))
                    u_price = float(r.get("unit_price", 0.0))
                    line_tot = float(r.get("line_total", 0.0))

                    if qty_ord <= 0 or qty_rec < 0 or qty_rec > qty_ord:
                        invalid_qty.append({
                            "line": idx, "item_id": r.get("order_item_id"),
                            "issue": f"Invalid quantities: ordered={qty_ord}, received={qty_rec}"
                        })

                    expected_tot = round(qty_ord * u_price, 2)
                    if abs(expected_tot - line_tot) > 0.02:
                        math_mismatch.append({
                            "line": idx, "item_id": r.get("order_item_id"),
                            "issue": f"Extended line total mismatch: {line_tot} != expected {expected_tot}"
                        })
                except ValueError:
                    pass

            self._add_check(
                check_id="po_items_valid_quantities",
                dataset="purchase_order_items.csv",
                check_name="PO Item Quantities Validity",
                severity=CheckSeverity.ERROR,
                total_records=len(poi_rows),
                failed_records=len(invalid_qty),
                details=f"Found {len(invalid_qty)} items with invalid ordered or received quantities.",
                sample_issues=invalid_qty,
            )

            self._add_check(
                check_id="po_items_line_total_math",
                dataset="purchase_order_items.csv",
                check_name="Line Total Calculation Consistency",
                severity=CheckSeverity.ERROR,
                total_records=len(poi_rows),
                failed_records=len(math_mismatch),
                details=f"Found {len(math_mismatch)} items where line_total differs from quantity * unit_price.",
                sample_issues=math_mismatch,
            )

        # 4. Product Margin & Stock Level Consistency (ERROR)
        if "products.csv" in datasets and datasets["products.csv"]:
            prd_rows = datasets["products.csv"]
            margin_errors = []
            stock_thresh_errors = []

            for idx, r in enumerate(prd_rows, start=2):
                try:
                    cost = float(r.get("unit_cost", 0.0))
                    price = float(r.get("unit_price", 0.0))
                    reorder_pt = int(r.get("reorder_point", 0))
                    target_stk = int(r.get("target_stock_level", 0))

                    if price < cost:
                        margin_errors.append({
                            "line": idx, "product_id": r.get("product_id"),
                            "issue": f"Unit price ({price}) is lower than acquisition cost ({cost})"
                        })

                    if target_stk <= reorder_pt:
                        stock_thresh_errors.append({
                            "line": idx, "product_id": r.get("product_id"),
                            "issue": f"Target stock ({target_stk}) <= Reorder point ({reorder_pt})"
                        })
                except ValueError:
                    pass

            self._add_check(
                check_id="products_positive_profit_margin",
                dataset="products.csv",
                check_name="Positive Product Price Margin",
                severity=CheckSeverity.ERROR,
                total_records=len(prd_rows),
                failed_records=len(margin_errors),
                details=f"Found {len(margin_errors)} products priced below unit acquisition cost.",
                sample_issues=margin_errors,
            )

            self._add_check(
                check_id="products_target_gt_reorder_point",
                dataset="products.csv",
                check_name="Target Stock > Reorder Point",
                severity=CheckSeverity.ERROR,
                total_records=len(prd_rows),
                failed_records=len(stock_thresh_errors),
                details=f"Found {len(stock_thresh_errors)} products where target stock is not greater than reorder point.",
                sample_issues=stock_thresh_errors,
            )

    def validate_all(self) -> ValidationReport:
        """Executes full suite of schema, key, referential, and business rule validations.

        Returns:
            ValidationReport detailing all checks and summary counts.
        """
        self.checks = []
        self.metrics = {}
        loaded_datasets: Dict[str, List[Dict[str, str]]] = {}

        self.logger.info("Initiating RetailFlow Data Quality Audit on: %s", self.data_dir)

        # 1. Individual dataset structural and column validations
        for filename, contract in self.contracts.items():
            rows = self.validate_file_presence_and_structure(filename, contract)
            if rows is not None:
                loaded_datasets[filename] = rows
                self.validate_column_contracts(filename, contract, rows)
                self.validate_keys(filename, contract, rows)

        # 2. Relational integrity across all datasets
        self.validate_referential_integrity(loaded_datasets)

        # 3. Retail supply chain business rules
        self.validate_business_rules(loaded_datasets)

        # Compile report metrics
        total_checks = len(self.checks)
        passed_checks = sum(1 for c in self.checks if c.status == CheckStatus.PASSED)
        failed_checks = sum(1 for c in self.checks if c.status == CheckStatus.FAILED)
        warning_checks = sum(1 for c in self.checks if c.status == CheckStatus.WARNING)
        critical_errors = sum(c.failed_records for c in self.checks if c.severity == CheckSeverity.ERROR)
        warnings = sum(c.failed_records for c in self.checks if c.severity == CheckSeverity.WARNING)

        overall_status = "FAILED" if failed_checks > 0 else "PASSED"

        report = ValidationReport(
            validation_timestamp_utc=datetime.now(timezone.utc).isoformat(),
            data_directory=str(self.data_dir.resolve()),
            overall_status=overall_status,
            total_checks=total_checks,
            passed_checks=passed_checks,
            warning_checks=warning_checks,
            failed_checks=failed_checks,
            critical_errors=critical_errors,
            warnings=warnings,
            metrics=self.metrics,
            checks=self.checks,
        )

        self.logger.info(
            "Data Quality Audit Completed: Overall Status=%s (Checks: %d Passed, %d Warnings, %d Failed. Critical Errors: %d)",
            overall_status,
            passed_checks,
            warning_checks,
            failed_checks,
            critical_errors,
        )

        return report

    def save_report(self, report: ValidationReport, output_path: Union[str, Path]) -> Path:
        """Saves machine-readable JSON validation report.

        Args:
            report: ValidationReport instance.
            output_path: Target path for JSON file.

        Returns:
            Resolved Path of the saved report.
        """
        p = Path(output_path)
        out = p if p.is_absolute() else (PROJECT_ROOT / p).resolve()
        out.parent.mkdir(parents=True, exist_ok=True)

        with open(out, mode="w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2)

        self.logger.info("Data quality validation report saved to %s", out)
        return out


def main():
    """CLI entrypoint for executing RetailFlow data quality validation."""
    parser = argparse.ArgumentParser(description="RetailFlow Data Quality & Data Contracts Validator")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "bronze"),
        help="Path to directory containing CSV datasets (default: data/bronze)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=str(PROJECT_ROOT / "data" / "bronze" / "quality_report.json"),
        help="Path to save machine-readable JSON report",
    )
    parser.add_argument(
        "--fail-on-warnings",
        action="store_true",
        help="Fail validation with non-zero exit code if warnings exist",
    )

    args = parser.parse_args()

    print(f"Starting RetailFlow Data Quality Audit...")
    print(f"  Target Data Directory: {args.data_dir}")
    print(f"  Report Output Path:    {args.output}")

    validator = DataQualityValidator(data_dir=args.data_dir)
    report = validator.validate_all()
    validator.save_report(report, args.output)

    print("\n" + "=" * 65)
    print(f"DATA QUALITY AUDIT REPORT: {report.overall_status}")
    print("=" * 65)
    print(f"  Total Checks Evaluated : {report.total_checks}")
    print(f"  Passed Checks          : {report.passed_checks}")
    print(f"  Warning Checks         : {report.warning_checks}")
    print(f"  Failed Checks (Errors) : {report.failed_checks}")
    print(f"  Critical Record Errors : {report.critical_errors}")
    print(f"  Business Rule Warnings : {report.warnings}")
    print("-" * 65)
    print("  Key Business Metrics:")
    for k, v in report.metrics.items():
        print(f"    - {k:<28}: {v}")
    print("=" * 65)

    if report.failed_checks > 0:
        print("\nCRITICAL DATA CONTRACT VIOLATIONS DETECTED:")
        for c in report.checks:
            if c.status == CheckStatus.FAILED:
                print(f"  [X] {c.dataset:<26} | {c.check_name:<36} | {c.details}")
        sys.exit(1)

    if args.fail_on_warnings and report.warning_checks > 0:
        print("\nWARNINGS DETECTED AND --fail-on-warnings ACTIVE:")
        for c in report.checks:
            if c.status == CheckStatus.WARNING:
                print(f"  [!] {c.dataset:<26} | {c.check_name:<36} | {c.details}")
        sys.exit(1)

    print("\nAll data contracts and critical validation checks PASSED successfully.")
    sys.exit(0)


if __name__ == "__main__":
    main()
