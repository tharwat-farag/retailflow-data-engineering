"""PostgreSQL Data Warehouse Loader for RetailFlow.

Reads conformed Silver layer CSV datasets and loads them into PostgreSQL:
1. Operational Staging schema (`staging.stg_*`).
2. Conformed Date, Supplier, Warehouse, and Product dimensions (`analytics.dim_*`).
3. Analytical Star Schema facts (`analytics.fact_*`).

Features:
- Transactional atomicity: automatic rollback on connection or constraint failure.
- Idempotent upserts: re-running does not duplicate facts or dimensions.
- Topological foreign-key dependency resolution.
- Environment-driven configuration via python-dotenv.
"""

import argparse
import csv
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Set, Tuple, Union

# Define PROJECT_ROOT and configure sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Attempt to load .env variables
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.utils.logger import get_logger


class WarehouseLoadError(Exception):
    """Base exception for data warehouse loading failures."""
    pass


class WarehouseConnectionError(WarehouseLoadError):
    """Raised when database connection cannot be established."""
    pass


class WarehouseTransactionError(WarehouseLoadError):
    """Raised when an SQL execution or constraint failure occurs within a transaction."""
    pass


class WarehouseConfig:
    """Encapsulates PostgreSQL warehouse connection configuration."""

    def __init__(
        self,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[Union[str, int]] = None,
        database: Optional[str] = None,
        db_url: Optional[str] = None,
    ):
        self.user = user or os.getenv("POSTGRES_USER", "retailflow_admin")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "retailflow_secure_pass")
        self.host = host or os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(port or os.getenv("POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("POSTGRES_DB", "retailflow_warehouse")
        self._custom_url = db_url

    @property
    def connection_url(self) -> str:
        """Constructs the standard SQLAlchemy connection URL."""
        if self._custom_url:
            return self._custom_url
        return f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"

    def get_engine(self, **kwargs) -> Engine:
        """Creates and returns an active SQLAlchemy Engine."""
        return create_engine(self.connection_url, **kwargs)


class WarehouseLoader:
    """Coordinates the staging, dimensional conforming, and fact loading pipeline."""

    def __init__(
        self,
        silver_dir: Optional[Union[str, Path]] = None,
        config: Optional[WarehouseConfig] = None,
        engine: Optional[Engine] = None,
    ):
        """Initializes the warehouse loader.

        Args:
            silver_dir: Directory containing conformed Silver CSV files.
            config: WarehouseConfig instance with database connection details.
            engine: Optional pre-configured SQLAlchemy engine (useful for testing).
        """
        self.logger = get_logger("retailflow.warehouse")
        default_silver = PROJECT_ROOT / "data" / "silver"

        if silver_dir is None:
            self.silver_dir = default_silver
        else:
            p = Path(silver_dir)
            self.silver_dir = p if p.is_absolute() else (PROJECT_ROOT / p).resolve()

        self.config = config or WarehouseConfig()
        self.engine = engine or self.config.get_engine(pool_pre_ping=True)

    def _read_csv(self, filename: str) -> List[Dict[str, str]]:
        """Reads a Silver CSV dataset in read-only mode."""
        file_path = self.silver_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Required Silver dataset '{filename}' not found at {file_path}")

        with open(file_path, mode="r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def test_connection(self) -> bool:
        """Verifies database connectivity before executing loads."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.logger.info("Database connection test succeeded.")
            return True
        except Exception as exc:
            self.logger.error("Database connectivity check failed: %s", exc)
            raise WarehouseConnectionError(f"Failed to connect to database: {exc}") from exc

    def initialize_schema(self, schema_sql_path: Optional[Union[str, Path]] = None) -> None:
        """Executes DDL statements to ensure staging and analytics schemas exist."""
        sql_file = Path(schema_sql_path) if schema_sql_path else (PROJECT_ROOT / "sql" / "schema" / "01_init_warehouse.sql")
        if not sql_file.exists():
            raise FileNotFoundError(f"Schema file not found at {sql_file}")

        self.logger.info("Initializing warehouse schema from %s", sql_file)
        ddl_content = sql_file.read_text(encoding="utf-8")

        # Split statements on semicolons, ignoring comments
        statements = [
            s.strip() for s in ddl_content.split(";")
            if s.strip() and not s.strip().startswith("--")
        ]

        with self.engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))

        self.logger.info("Schema DDL initialization completed successfully.")

    # -------------------------------------------------------------------------
    # Dimensional Date Generation
    # -------------------------------------------------------------------------

    @staticmethod
    def generate_date_dimension_records(dates: Set[str]) -> List[Dict[str, Any]]:
        """Generates conformed calendar dimension records from a set of date strings."""
        records = []
        for d_str in sorted(dates):
            if not d_str or not d_str.strip():
                continue
            dt = datetime.strptime(d_str.strip(), "%Y-%m-%d").date()
            date_key = int(dt.strftime("%Y%m%d"))
            records.append({
                "date_key": date_key,
                "full_date": dt,
                "year": dt.year,
                "quarter": (dt.month - 1) // 3 + 1,
                "month": dt.month,
                "month_name": dt.strftime("%B"),
                "day": dt.day,
                "day_of_week": dt.isoweekday(),
                "day_name": dt.strftime("%A"),
                "is_weekend": dt.isoweekday() in (6, 7),
            })
        return records

    # -------------------------------------------------------------------------
    # Transactional Loading Logic
    # -------------------------------------------------------------------------

    def load_all(self, init_schema: bool = True) -> Dict[str, Any]:
        """Loads all Silver datasets into staging and analytical star schema tables.

        Execution is wrapped in an atomic database transaction. If any error
        occurs, the entire batch is rolled back automatically.

        Args:
            init_schema: Whether to run schema initialization DDL first.

        Returns:
            Dictionary summary of loaded record counts across all schemas.

        Raises:
            WarehouseConnectionError: If database is unreachable.
            WarehouseTransactionError: If an error occurs during data loading.
        """
        self.logger.info("Starting warehouse load from Silver directory: %s", self.silver_dir)

        # 1. Connectivity check
        self.test_connection()

        # 2. Schema check/init
        if init_schema:
            self.initialize_schema()

        # 3. Read Silver inputs
        silver_suppliers = self._read_csv("suppliers.csv")
        silver_warehouses = self._read_csv("warehouses.csv")
        silver_products = self._read_csv("products.csv")
        silver_inventory = self._read_csv("inventory.csv")
        silver_pos = self._read_csv("purchase_orders.csv")
        silver_poi = self._read_csv("purchase_order_items.csv")

        # Extract distinct dates for dim_date
        all_dates: Set[str] = set()
        for r in silver_inventory:
            all_dates.add(r["last_counted_date"])
        for r in silver_pos:
            all_dates.add(r["order_date"])
            all_dates.add(r["expected_delivery_date"])
            if r.get("actual_delivery_date"):
                all_dates.add(r["actual_delivery_date"])

        date_records = self.generate_date_dimension_records(all_dates)

        summary: Dict[str, Any] = {
            "loaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "SUCCESS",
            "staging": {},
            "dimensions": {},
            "facts": {},
        }

        # 4. Atomic Transactional Execution
        try:
            with self.engine.begin() as conn:
                self.logger.info("Transaction opened. Loading staging tables...")

                # A. Staging Tables (Upsert/Replace)
                # stg_suppliers
                for s in silver_suppliers:
                    conn.execute(
                        text("""
                            INSERT INTO staging.stg_suppliers (
                                supplier_id, supplier_name, contact_email, country,
                                reliability_score, lead_time_days, reliability_tier
                            ) VALUES (
                                :supplier_id, :supplier_name, :contact_email, :country,
                                :reliability_score, :lead_time_days, :reliability_tier
                            ) ON CONFLICT (supplier_id) DO UPDATE SET
                                supplier_name = EXCLUDED.supplier_name,
                                contact_email = EXCLUDED.contact_email,
                                country = EXCLUDED.country,
                                reliability_score = EXCLUDED.reliability_score,
                                lead_time_days = EXCLUDED.lead_time_days,
                                reliability_tier = EXCLUDED.reliability_tier;
                        """),
                        s
                    )
                summary["staging"]["stg_suppliers"] = len(silver_suppliers)

                # stg_warehouses
                for w in silver_warehouses:
                    conn.execute(
                        text("""
                            INSERT INTO staging.stg_warehouses (
                                warehouse_id, warehouse_name, city, state, country, capacity_units
                            ) VALUES (
                                :warehouse_id, :warehouse_name, :city, :state, :country, :capacity_units
                            ) ON CONFLICT (warehouse_id) DO UPDATE SET
                                warehouse_name = EXCLUDED.warehouse_name,
                                city = EXCLUDED.city,
                                state = EXCLUDED.state,
                                country = EXCLUDED.country,
                                capacity_units = EXCLUDED.capacity_units;
                        """),
                        w
                    )
                summary["staging"]["stg_warehouses"] = len(silver_warehouses)

                # stg_products
                for p in silver_products:
                    conn.execute(
                        text("""
                            INSERT INTO staging.stg_products (
                                product_id, product_name, category, subcategory, unit_cost,
                                unit_price, margin_amount, margin_percentage, reorder_point, target_stock_level
                            ) VALUES (
                                :product_id, :product_name, :category, :subcategory, :unit_cost,
                                :unit_price, :margin_amount, :margin_percentage, :reorder_point, :target_stock_level
                            ) ON CONFLICT (product_id) DO UPDATE SET
                                product_name = EXCLUDED.product_name,
                                category = EXCLUDED.category,
                                subcategory = EXCLUDED.subcategory,
                                unit_cost = EXCLUDED.unit_cost,
                                unit_price = EXCLUDED.unit_price,
                                margin_amount = EXCLUDED.margin_amount,
                                margin_percentage = EXCLUDED.margin_percentage,
                                reorder_point = EXCLUDED.reorder_point,
                                target_stock_level = EXCLUDED.target_stock_level;
                        """),
                        p
                    )
                summary["staging"]["stg_products"] = len(silver_products)

                # stg_inventory
                for inv in silver_inventory:
                    conn.execute(
                        text("""
                            INSERT INTO staging.stg_inventory (
                                inventory_id, warehouse_id, product_id, stock_on_hand,
                                stock_allocated, stock_reserved, stock_available, inventory_status, last_counted_date
                            ) VALUES (
                                :inventory_id, :warehouse_id, :product_id, :stock_on_hand,
                                :stock_allocated, :stock_reserved, :stock_available, :inventory_status, :last_counted_date
                            ) ON CONFLICT (inventory_id) DO UPDATE SET
                                warehouse_id = EXCLUDED.warehouse_id,
                                product_id = EXCLUDED.product_id,
                                stock_on_hand = EXCLUDED.stock_on_hand,
                                stock_allocated = EXCLUDED.stock_allocated,
                                stock_reserved = EXCLUDED.stock_reserved,
                                stock_available = EXCLUDED.stock_available,
                                inventory_status = EXCLUDED.inventory_status,
                                last_counted_date = EXCLUDED.last_counted_date;
                        """),
                        inv
                    )
                summary["staging"]["stg_inventory"] = len(silver_inventory)

                # stg_purchase_orders
                for po in silver_pos:
                    po_dict = po.copy()
                    po_dict["actual_delivery_date"] = po["actual_delivery_date"] if po["actual_delivery_date"] else None
                    po_dict["delivery_delay_days"] = int(po["delivery_delay_days"]) if po["delivery_delay_days"] != "" else None
                    po_dict["is_delayed"] = (po["is_delayed"].upper() == "TRUE") if po["is_delayed"] != "" else None
                    po_dict["fulfillment_lead_time_days"] = int(po["fulfillment_lead_time_days"]) if po["fulfillment_lead_time_days"] != "" else None

                    conn.execute(
                        text("""
                            INSERT INTO staging.stg_purchase_orders (
                                order_id, supplier_id, warehouse_id, order_date, expected_delivery_date,
                                actual_delivery_date, order_status, total_amount, delivery_delay_days,
                                is_delayed, fulfillment_lead_time_days
                            ) VALUES (
                                :order_id, :supplier_id, :warehouse_id, :order_date, :expected_delivery_date,
                                :actual_delivery_date, :order_status, :total_amount, :delivery_delay_days,
                                :is_delayed, :fulfillment_lead_time_days
                            ) ON CONFLICT (order_id) DO UPDATE SET
                                supplier_id = EXCLUDED.supplier_id,
                                warehouse_id = EXCLUDED.warehouse_id,
                                order_date = EXCLUDED.order_date,
                                expected_delivery_date = EXCLUDED.expected_delivery_date,
                                actual_delivery_date = EXCLUDED.actual_delivery_date,
                                order_status = EXCLUDED.order_status,
                                total_amount = EXCLUDED.total_amount,
                                delivery_delay_days = EXCLUDED.delivery_delay_days,
                                is_delayed = EXCLUDED.is_delayed,
                                fulfillment_lead_time_days = EXCLUDED.fulfillment_lead_time_days;
                        """),
                        po_dict
                    )
                summary["staging"]["stg_purchase_orders"] = len(silver_pos)

                # stg_purchase_order_items
                for poi in silver_poi:
                    conn.execute(
                        text("""
                            INSERT INTO staging.stg_purchase_order_items (
                                order_item_id, order_id, product_id, quantity_ordered,
                                quantity_received, quantity_pending, fulfillment_ratio, unit_price, line_total
                            ) VALUES (
                                :order_item_id, :order_id, :product_id, :quantity_ordered,
                                :quantity_received, :quantity_pending, :fulfillment_ratio, :unit_price, :line_total
                            ) ON CONFLICT (order_item_id) DO UPDATE SET
                                order_id = EXCLUDED.order_id,
                                product_id = EXCLUDED.product_id,
                                quantity_ordered = EXCLUDED.quantity_ordered,
                                quantity_received = EXCLUDED.quantity_received,
                                quantity_pending = EXCLUDED.quantity_pending,
                                fulfillment_ratio = EXCLUDED.fulfillment_ratio,
                                unit_price = EXCLUDED.unit_price,
                                line_total = EXCLUDED.line_total;
                        """),
                        poi
                    )
                summary["staging"]["stg_purchase_order_items"] = len(silver_poi)

                # B. Dimensions (analytics.dim_*)
                self.logger.info("Loading Dimension tables...")

                # dim_date
                for d_rec in date_records:
                    conn.execute(
                        text("""
                            INSERT INTO analytics.dim_date (
                                date_key, full_date, year, quarter, month, month_name,
                                day, day_of_week, day_name, is_weekend
                            ) VALUES (
                                :date_key, :full_date, :year, :quarter, :month, :month_name,
                                :day, :day_of_week, :day_name, :is_weekend
                            ) ON CONFLICT (date_key) DO NOTHING;
                        """),
                        d_rec
                    )
                summary["dimensions"]["dim_date"] = len(date_records)

                # dim_supplier
                for s in silver_suppliers:
                    conn.execute(
                        text("""
                            INSERT INTO analytics.dim_supplier (
                                supplier_id, supplier_name, contact_email, country,
                                reliability_score, lead_time_days, reliability_tier
                            ) VALUES (
                                :supplier_id, :supplier_name, :contact_email, :country,
                                :reliability_score, :lead_time_days, :reliability_tier
                            ) ON CONFLICT (supplier_id) DO UPDATE SET
                                supplier_name = EXCLUDED.supplier_name,
                                contact_email = EXCLUDED.contact_email,
                                country = EXCLUDED.country,
                                reliability_score = EXCLUDED.reliability_score,
                                lead_time_days = EXCLUDED.lead_time_days,
                                reliability_tier = EXCLUDED.reliability_tier;
                        """),
                        s
                    )
                summary["dimensions"]["dim_supplier"] = len(silver_suppliers)

                # dim_warehouse
                for w in silver_warehouses:
                    conn.execute(
                        text("""
                            INSERT INTO analytics.dim_warehouse (
                                warehouse_id, warehouse_name, city, state, country, capacity_units
                            ) VALUES (
                                :warehouse_id, :warehouse_name, :city, :state, :country, :capacity_units
                            ) ON CONFLICT (warehouse_id) DO UPDATE SET
                                warehouse_name = EXCLUDED.warehouse_name,
                                city = EXCLUDED.city,
                                state = EXCLUDED.state,
                                country = EXCLUDED.country,
                                capacity_units = EXCLUDED.capacity_units;
                        """),
                        w
                    )
                summary["dimensions"]["dim_warehouse"] = len(silver_warehouses)

                # dim_product
                for p in silver_products:
                    conn.execute(
                        text("""
                            INSERT INTO analytics.dim_product (
                                product_id, product_name, category, subcategory, unit_cost,
                                unit_price, margin_amount, margin_percentage, reorder_point, target_stock_level
                            ) VALUES (
                                :product_id, :product_name, :category, :subcategory, :unit_cost,
                                :unit_price, :margin_amount, :margin_percentage, :reorder_point, :target_stock_level
                            ) ON CONFLICT (product_id) DO UPDATE SET
                                product_name = EXCLUDED.product_name,
                                category = EXCLUDED.category,
                                subcategory = EXCLUDED.subcategory,
                                unit_cost = EXCLUDED.unit_cost,
                                unit_price = EXCLUDED.unit_price,
                                margin_amount = EXCLUDED.margin_amount,
                                margin_percentage = EXCLUDED.margin_percentage,
                                reorder_point = EXCLUDED.reorder_point,
                                target_stock_level = EXCLUDED.target_stock_level;
                        """),
                        p
                    )
                summary["dimensions"]["dim_product"] = len(silver_products)

                # C. Build Surrogate Key Lookups
                self.logger.info("Resolving surrogate key lookups for Fact loading...")
                sup_lookup = {
                    row[0]: row[1]
                    for row in conn.execute(text("SELECT supplier_id, supplier_key FROM analytics.dim_supplier")).fetchall()
                }
                wh_lookup = {
                    row[0]: row[1]
                    for row in conn.execute(text("SELECT warehouse_id, warehouse_key FROM analytics.dim_warehouse")).fetchall()
                }
                prod_lookup = {
                    row[0]: row[1]
                    for row in conn.execute(text("SELECT product_id, product_key FROM analytics.dim_product")).fetchall()
                }

                # D. Facts (analytics.fact_*)
                # fact_inventory_snapshot
                self.logger.info("Loading fact_inventory_snapshot...")
                for inv in silver_inventory:
                    c_date = inv["last_counted_date"]
                    d_key = int(datetime.strptime(c_date, "%Y-%m-%d").strftime("%Y%m%d"))
                    w_key = wh_lookup[inv["warehouse_id"]]
                    p_key = prod_lookup[inv["product_id"]]

                    fact_inv_record = {
                        "inventory_id": inv["inventory_id"],
                        "date_key": d_key,
                        "warehouse_key": w_key,
                        "product_key": p_key,
                        "stock_on_hand": int(inv["stock_on_hand"]),
                        "stock_allocated": int(inv["stock_allocated"]),
                        "stock_reserved": int(inv["stock_reserved"]),
                        "stock_available": int(inv["stock_available"]),
                        "inventory_status": inv["inventory_status"],
                        "last_counted_date": c_date,
                    }

                    conn.execute(
                        text("""
                            INSERT INTO analytics.fact_inventory_snapshot (
                                inventory_id, date_key, warehouse_key, product_key,
                                stock_on_hand, stock_allocated, stock_reserved,
                                stock_available, inventory_status, last_counted_date
                            ) VALUES (
                                :inventory_id, :date_key, :warehouse_key, :product_key,
                                :stock_on_hand, :stock_allocated, :stock_reserved,
                                :stock_available, :inventory_status, :last_counted_date
                            ) ON CONFLICT (inventory_id, date_key) DO UPDATE SET
                                stock_on_hand = EXCLUDED.stock_on_hand,
                                stock_allocated = EXCLUDED.stock_allocated,
                                stock_reserved = EXCLUDED.stock_reserved,
                                stock_available = EXCLUDED.stock_available,
                                inventory_status = EXCLUDED.inventory_status;
                        """),
                        fact_inv_record
                    )
                summary["facts"]["fact_inventory_snapshot"] = len(silver_inventory)

                # fact_purchase_order_fulfillment
                self.logger.info("Loading fact_purchase_order_fulfillment...")
                po_lookup = {po["order_id"]: po for po in silver_pos}

                for poi in silver_poi:
                    parent_po = po_lookup[poi["order_id"]]
                    ord_d_key = int(datetime.strptime(parent_po["order_date"], "%Y-%m-%d").strftime("%Y%m%d"))
                    exp_d_key = int(datetime.strptime(parent_po["expected_delivery_date"], "%Y-%m-%d").strftime("%Y%m%d"))

                    if parent_po.get("actual_delivery_date"):
                        act_d_key = int(datetime.strptime(parent_po["actual_delivery_date"], "%Y-%m-%d").strftime("%Y%m%d"))
                    else:
                        act_d_key = None

                    s_key = sup_lookup[parent_po["supplier_id"]]
                    w_key = wh_lookup[parent_po["warehouse_id"]]
                    p_key = prod_lookup[poi["product_id"]]

                    delay_days = int(parent_po["delivery_delay_days"]) if parent_po.get("delivery_delay_days") != "" else None
                    is_delayed = (parent_po.get("is_delayed", "").upper() == "TRUE") if parent_po.get("is_delayed") != "" else None
                    lead_time = int(parent_po["fulfillment_lead_time_days"]) if parent_po.get("fulfillment_lead_time_days") != "" else None

                    fact_po_record = {
                        "order_item_id": poi["order_item_id"],
                        "order_id": poi["order_id"],
                        "order_date_key": ord_d_key,
                        "expected_delivery_date_key": exp_d_key,
                        "actual_delivery_date_key": act_d_key,
                        "supplier_key": s_key,
                        "warehouse_key": w_key,
                        "product_key": p_key,
                        "order_status": parent_po["order_status"],
                        "quantity_ordered": int(poi["quantity_ordered"]),
                        "quantity_received": int(poi["quantity_received"]),
                        "quantity_pending": int(poi["quantity_pending"]),
                        "fulfillment_ratio": float(poi["fulfillment_ratio"]),
                        "unit_price": float(poi["unit_price"]),
                        "line_total": float(poi["line_total"]),
                        "delivery_delay_days": delay_days,
                        "is_delayed": is_delayed,
                        "fulfillment_lead_time_days": lead_time,
                    }

                    conn.execute(
                        text("""
                            INSERT INTO analytics.fact_purchase_order_fulfillment (
                                order_item_id, order_id, order_date_key, expected_delivery_date_key,
                                actual_delivery_date_key, supplier_key, warehouse_key, product_key,
                                order_status, quantity_ordered, quantity_received, quantity_pending,
                                fulfillment_ratio, unit_price, line_total, delivery_delay_days,
                                is_delayed, fulfillment_lead_time_days
                            ) VALUES (
                                :order_item_id, :order_id, :order_date_key, :expected_delivery_date_key,
                                :actual_delivery_date_key, :supplier_key, :warehouse_key, :product_key,
                                :order_status, :quantity_ordered, :quantity_received, :quantity_pending,
                                :fulfillment_ratio, :unit_price, :line_total, :delivery_delay_days,
                                :is_delayed, :fulfillment_lead_time_days
                            ) ON CONFLICT (order_item_id) DO UPDATE SET
                                quantity_received = EXCLUDED.quantity_received,
                                quantity_pending = EXCLUDED.quantity_pending,
                                fulfillment_ratio = EXCLUDED.fulfillment_ratio,
                                actual_delivery_date_key = EXCLUDED.actual_delivery_date_key,
                                order_status = EXCLUDED.order_status,
                                delivery_delay_days = EXCLUDED.delivery_delay_days,
                                is_delayed = EXCLUDED.is_delayed,
                                fulfillment_lead_time_days = EXCLUDED.fulfillment_lead_time_days;
                        """),
                        fact_po_record
                    )
                summary["facts"]["fact_purchase_order_fulfillment"] = len(silver_poi)

                self.logger.info("Transaction committed successfully. All tables loaded.")

        except Exception as exc:
            self.logger.error("Database transaction failed! Rollback executed. Error: %s", exc)
            raise WarehouseTransactionError(f"Warehouse transaction failed and was rolled back: {exc}") from exc

        return summary


def main():
    """CLI entrypoint for loading Silver datasets into PostgreSQL Data Warehouse."""
    parser = argparse.ArgumentParser(description="RetailFlow PostgreSQL Data Warehouse Loader")
    parser.add_argument(
        "--silver-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "silver"),
        help="Path to Silver datasets directory (default: data/silver)",
    )
    parser.add_argument(
        "--no-init-schema",
        action="store_true",
        help="Skip schema initialization DDL check",
    )

    args = parser.parse_args()

    print("=" * 65)
    print("RETAILFLOW POSTGRESQL DATA WAREHOUSE LOADER")
    print("=" * 65)
    print(f"  Silver Directory: {args.silver_dir}")

    config = WarehouseConfig()
    print(f"  Target Database : {config.user}@{config.host}:{config.port}/{config.database}")
    print("-" * 65)

    loader = WarehouseLoader(silver_dir=args.silver_dir, config=config)

    try:
        summary = loader.load_all(init_schema=not args.no_init_schema)
    except WarehouseConnectionError as conn_err:
        print("\n[CONNECTION ERROR] Could not connect to PostgreSQL!")
        print(f"Details: {conn_err}")
        print("\nPlease ensure Docker Desktop is running and PostgreSQL container is active:")
        print("  docker compose up -d postgres")
        sys.exit(1)
    except WarehouseLoadError as load_err:
        print(f"\n[LOAD ERROR] Warehouse load failed: {load_err}")
        sys.exit(1)
    except Exception as exc:
        print(f"\n[UNEXPECTED ERROR] {exc}")
        sys.exit(1)

    print("\nWarehouse Load Succeeded!")
    print(f"  Loaded at (UTC) : {summary['loaded_at_utc']}")
    print("-" * 65)
    print("  Staging Tables Loaded:")
    for tbl, count in summary["staging"].items():
        print(f"    - staging.{tbl:<28}: {count:>5} rows")
    print("  Dimension Tables Loaded:")
    for tbl, count in summary["dimensions"].items():
        print(f"    - analytics.{tbl:<26}: {count:>5} rows")
    print("  Fact Tables Loaded:")
    for tbl, count in summary["facts"].items():
        print(f"    - analytics.{tbl:<26}: {count:>5} rows")
    print("=" * 65)

    sys.exit(0)


if __name__ == "__main__":
    main()
