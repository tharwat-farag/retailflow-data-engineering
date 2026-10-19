"""Data Contracts and Schema Specifications for RetailFlow.

Defines schemas, data types, constraints, and relational integrity rules
for all six supply chain datasets in the RetailFlow pipeline.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


@dataclass
class ColumnContract:
    """Specification and validation rules for a single dataset column.

    Attributes:
        name: Name of the column in the CSV.
        data_type: Expected logical data type ('string', 'int', 'float', 'date').
        required: Whether the column must be present in the CSV header.
        nullable: Whether empty/null values are permitted.
        min_value: Optional minimum numeric value (inclusive).
        max_value: Optional maximum numeric value (inclusive).
        allowed_values: Optional set of allowed categorical string values.
        date_format: Expected date format string (e.g. '%Y-%m-%d').
        description: Beginner-friendly description of the field.
    """
    name: str
    data_type: str = "string"
    required: bool = True
    nullable: bool = False
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[Set[str]] = None
    date_format: Optional[str] = "%Y-%m-%d"
    description: str = ""


@dataclass
class ForeignKeyContract:
    """Foreign key relationship specification between datasets.

    Attributes:
        column: Column in the current (child) dataset.
        parent_dataset: Filename of the referenced parent dataset.
        parent_column: Key column in the parent dataset.
        description: Description of the relationship.
    """
    column: str
    parent_dataset: str
    parent_column: str
    description: str = ""


@dataclass
class TableContract:
    """Full schema contract for a dataset table.

    Attributes:
        dataset_name: Name of the CSV file (e.g. 'products.csv').
        primary_key: Primary key column name.
        columns: Dictionary mapping column name to ColumnContract.
        foreign_keys: List of ForeignKeyContract objects.
        composite_keys: Optional list of tuples defining unique composite keys.
        description: Business purpose of this dataset.
    """
    dataset_name: str
    primary_key: str
    columns: Dict[str, ColumnContract]
    foreign_keys: List[ForeignKeyContract] = field(default_factory=list)
    composite_keys: List[Tuple[str, ...]] = field(default_factory=list)
    description: str = ""


# Contract Definitions for all 6 RetailFlow Datasets
SUPPLIERS_CONTRACT = TableContract(
    dataset_name="suppliers.csv",
    primary_key="supplier_id",
    description="International and domestic product suppliers",
    columns={
        "supplier_id": ColumnContract(
            name="supplier_id", data_type="string", required=True, nullable=False,
            description="Unique identifier for supplier (e.g. SUP001)"
        ),
        "supplier_name": ColumnContract(
            name="supplier_name", data_type="string", required=True, nullable=False,
            description="Full legal business name of supplier"
        ),
        "contact_email": ColumnContract(
            name="contact_email", data_type="string", required=True, nullable=False,
            description="Primary supply coordinator contact email"
        ),
        "country": ColumnContract(
            name="country", data_type="string", required=True, nullable=False,
            description="Country of primary manufacturing or distribution"
        ),
        "reliability_score": ColumnContract(
            name="reliability_score", data_type="float", required=True, nullable=False,
            min_value=0.0, max_value=1.0,
            description="Historical fulfillment reliability score between 0.0 and 1.0"
        ),
        "lead_time_days": ColumnContract(
            name="lead_time_days", data_type="int", required=True, nullable=False,
            min_value=1,
            description="Standard transit lead time in calendar days"
        ),
    },
)

WAREHOUSES_CONTRACT = TableContract(
    dataset_name="warehouses.csv",
    primary_key="warehouse_id",
    description="Regional distribution centers and fulfillment facilities",
    columns={
        "warehouse_id": ColumnContract(
            name="warehouse_id", data_type="string", required=True, nullable=False,
            description="Unique warehouse code (e.g. WH001)"
        ),
        "warehouse_name": ColumnContract(
            name="warehouse_name", data_type="string", required=True, nullable=False,
            description="Descriptive facility name"
        ),
        "city": ColumnContract(name="city", data_type="string", required=True, nullable=False),
        "state": ColumnContract(name="state", data_type="string", required=True, nullable=False),
        "country": ColumnContract(name="country", data_type="string", required=True, nullable=False),
        "capacity_units": ColumnContract(
            name="capacity_units", data_type="int", required=True, nullable=False,
            min_value=1,
            description="Maximum storage capacity in total units"
        ),
    },
)

PRODUCTS_CONTRACT = TableContract(
    dataset_name="products.csv",
    primary_key="product_id",
    description="Retail master product catalog with wholesale and retail pricing",
    columns={
        "product_id": ColumnContract(
            name="product_id", data_type="string", required=True, nullable=False,
            description="Stock Keeping Unit / Product identifier (e.g. PRD0001)"
        ),
        "product_name": ColumnContract(name="product_name", data_type="string", required=True, nullable=False),
        "category": ColumnContract(name="category", data_type="string", required=True, nullable=False),
        "subcategory": ColumnContract(name="subcategory", data_type="string", required=True, nullable=False),
        "unit_cost": ColumnContract(
            name="unit_cost", data_type="float", required=True, nullable=False,
            min_value=0.01,
            description="Wholesale acquisition cost per unit in USD"
        ),
        "unit_price": ColumnContract(
            name="unit_price", data_type="float", required=True, nullable=False,
            min_value=0.01,
            description="MSRP retail selling price per unit in USD"
        ),
        "reorder_point": ColumnContract(
            name="reorder_point", data_type="int", required=True, nullable=False,
            min_value=1,
            description="Inventory threshold that triggers replenishments"
        ),
        "target_stock_level": ColumnContract(
            name="target_stock_level", data_type="int", required=True, nullable=False,
            min_value=1,
            description="Optimal stock level after replenishment"
        ),
    },
)

INVENTORY_CONTRACT = TableContract(
    dataset_name="inventory.csv",
    primary_key="inventory_id",
    description="Current inventory snapshots across all distribution hubs",
    foreign_keys=[
        ForeignKeyContract("warehouse_id", "warehouses.csv", "warehouse_id", "Warehouse holding stock"),
        ForeignKeyContract("product_id", "products.csv", "product_id", "Product stored"),
    ],
    composite_keys=[("warehouse_id", "product_id")],
    columns={
        "inventory_id": ColumnContract(
            name="inventory_id", data_type="string", required=True, nullable=False,
            description="Unique inventory record identifier"
        ),
        "warehouse_id": ColumnContract(name="warehouse_id", data_type="string", required=True, nullable=False),
        "product_id": ColumnContract(name="product_id", data_type="string", required=True, nullable=False),
        "stock_on_hand": ColumnContract(
            name="stock_on_hand", data_type="int", required=True, nullable=False,
            min_value=0,
            description="Physical units present in warehouse"
        ),
        "stock_allocated": ColumnContract(
            name="stock_allocated", data_type="int", required=True, nullable=False,
            min_value=0,
            description="Units committed to outbound customer orders"
        ),
        "stock_reserved": ColumnContract(
            name="stock_reserved", data_type="int", required=True, nullable=False,
            min_value=0,
            description="Units set aside for quality checks or staging"
        ),
        "last_counted_date": ColumnContract(
            name="last_counted_date", data_type="date", required=True, nullable=False,
            date_format="%Y-%m-%d",
            description="Date of last physical cycle count"
        ),
    },
)

PURCHASE_ORDERS_CONTRACT = TableContract(
    dataset_name="purchase_orders.csv",
    primary_key="order_id",
    description="Header records for supplier purchase orders",
    foreign_keys=[
        ForeignKeyContract("supplier_id", "suppliers.csv", "supplier_id", "Supplier fulfilling order"),
        ForeignKeyContract("warehouse_id", "warehouses.csv", "warehouse_id", "Destination warehouse"),
    ],
    columns={
        "order_id": ColumnContract(
            name="order_id", data_type="string", required=True, nullable=False,
            description="Unique purchase order identifier"
        ),
        "supplier_id": ColumnContract(name="supplier_id", data_type="string", required=True, nullable=False),
        "warehouse_id": ColumnContract(name="warehouse_id", data_type="string", required=True, nullable=False),
        "order_date": ColumnContract(
            name="order_date", data_type="date", required=True, nullable=False,
            date_format="%Y-%m-%d",
            description="Date purchase order was submitted to supplier"
        ),
        "expected_delivery_date": ColumnContract(
            name="expected_delivery_date", data_type="date", required=True, nullable=False,
            date_format="%Y-%m-%d",
            description="Agreed contractual delivery arrival date"
        ),
        "actual_delivery_date": ColumnContract(
            name="actual_delivery_date", data_type="date", required=True, nullable=True,
            date_format="%Y-%m-%d",
            description="Actual dock arrival date (null for pending/cancelled orders)"
        ),
        "order_status": ColumnContract(
            name="order_status", data_type="string", required=True, nullable=False,
            allowed_values={"PENDING", "RECEIVED", "PARTIALLY_RECEIVED", "CANCELLED"},
            description="Current lifecycle status of purchase order"
        ),
        "total_amount": ColumnContract(
            name="total_amount", data_type="float", required=True, nullable=False,
            min_value=0.0,
            description="Sum total order valuation in USD"
        ),
    },
)

PURCHASE_ORDER_ITEMS_CONTRACT = TableContract(
    dataset_name="purchase_order_items.csv",
    primary_key="order_item_id",
    description="Line item records for purchase orders",
    foreign_keys=[
        ForeignKeyContract("order_id", "purchase_orders.csv", "order_id", "Parent purchase order"),
        ForeignKeyContract("product_id", "products.csv", "product_id", "Product ordered"),
    ],
    composite_keys=[("order_id", "product_id")],
    columns={
        "order_item_id": ColumnContract(
            name="order_item_id", data_type="string", required=True, nullable=False,
            description="Unique line item identifier"
        ),
        "order_id": ColumnContract(name="order_id", data_type="string", required=True, nullable=False),
        "product_id": ColumnContract(name="product_id", data_type="string", required=True, nullable=False),
        "quantity_ordered": ColumnContract(
            name="quantity_ordered", data_type="int", required=True, nullable=False,
            min_value=1,
            description="Units requested on purchase order"
        ),
        "quantity_received": ColumnContract(
            name="quantity_received", data_type="int", required=True, nullable=False,
            min_value=0,
            description="Units physically verified at warehouse dock"
        ),
        "unit_price": ColumnContract(
            name="unit_price", data_type="float", required=True, nullable=False,
            min_value=0.01,
            description="Negotiated purchase price per unit in USD"
        ),
        "line_total": ColumnContract(
            name="line_total", data_type="float", required=True, nullable=False,
            min_value=0.01,
            description="Line item extended total (quantity_ordered * unit_price)"
        ),
    },
)

ALL_CONTRACTS: Dict[str, TableContract] = {
    "suppliers.csv": SUPPLIERS_CONTRACT,
    "warehouses.csv": WAREHOUSES_CONTRACT,
    "products.csv": PRODUCTS_CONTRACT,
    "inventory.csv": INVENTORY_CONTRACT,
    "purchase_orders.csv": PURCHASE_ORDERS_CONTRACT,
    "purchase_order_items.csv": PURCHASE_ORDER_ITEMS_CONTRACT,
}
