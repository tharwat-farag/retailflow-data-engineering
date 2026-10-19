"""Synthetic Retail Supply Chain Data Generator.

Generates realistic CSV datasets for RetailFlow:
1. suppliers.csv
2. warehouses.csv
3. products.csv
4. inventory.csv
5. purchase_orders.csv
6. purchase_order_items.csv

Preserves strict referential integrity, reproducible random seeds,
and realistic supply chain scenarios (stockouts, delayed shipments, lead times).
"""

import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

# Define PROJECT_ROOT as the root of the retailflow repository
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class RetailDataGenerator:
    """Generates synthetic relational retail supply chain data."""

    def __init__(
        self,
        seed: int = 42,
        output_dir: Optional[Union[str, Path]] = None,
        sample_dir: Optional[Union[str, Path]] = None,
    ):
        self.seed = seed

        # Resolve output directories relative to project root (guaranteed outside src/)
        default_output = PROJECT_ROOT / "data" / "raw"
        default_sample = PROJECT_ROOT / "data" / "sample"

        if output_dir is None:
            self.output_dir = default_output
        else:
            p = Path(output_dir)
            self.output_dir = p if p.is_absolute() else (PROJECT_ROOT / p).resolve()

        if sample_dir is None:
            self.sample_dir = default_sample
        else:
            p = Path(sample_dir)
            self.sample_dir = p if p.is_absolute() else (PROJECT_ROOT / p).resolve()

        # Strict safeguard: ensure generated files are never written inside src/
        src_dir = (PROJECT_ROOT / "src").resolve()
        if self.output_dir.resolve().is_relative_to(src_dir):
            raise ValueError(f"Output directory cannot be located inside src/: {self.output_dir}")
        if self.sample_dir.resolve().is_relative_to(src_dir):
            raise ValueError(f"Sample directory cannot be located inside src/: {self.sample_dir}")

        self.rng = random.Random(seed)

        # Create output directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.sample_dir.mkdir(parents=True, exist_ok=True)

        # Storage for generated entities
        self.suppliers: List[Dict[str, Any]] = []
        self.warehouses: List[Dict[str, Any]] = []
        self.products: List[Dict[str, Any]] = []
        self.inventory: List[Dict[str, Any]] = []
        self.purchase_orders: List[Dict[str, Any]] = []
        self.purchase_order_items: List[Dict[str, Any]] = []

    def generate_suppliers(self, count: int = 25) -> List[Dict[str, Any]]:
        """Generates realistic international and domestic suppliers."""
        supplier_prefixes = [
            "Apex", "Nexus", "Summit", "Vanguard", "Pacific", "Atlantic",
            "Pinnacle", "Keystone", "Global", "Metro", "Prime", "Quantum",
            "Echo", "Horizon", "Sterling", "Ironclad", "Cascade", "Zenith"
        ]
        supplier_suffixes = [
            "Logistics Ltd", "Manufacturing Corp", "Industries Inc", "Supply Co",
            "Wholesale Partners", "Tech Components", "Global Trade", "Distribution", "Enterprises"
        ]
        countries = [
            ("USA", 0.40, 5, 12),
            ("China", 0.25, 20, 45),
            ("Germany", 0.10, 14, 28),
            ("Mexico", 0.10, 7, 18),
            ("Vietnam", 0.08, 18, 35),
            ("Canada", 0.07, 6, 14),
        ]

        self.suppliers = []
        used_names = set()

        for i in range(1, count + 1):
            supplier_id = f"SUP{i:03d}"

            attempts = 0
            while True:
                name = f"{self.rng.choice(supplier_prefixes)} {self.rng.choice(supplier_suffixes)}"
                if attempts > 50:
                    name = f"{name} {i}"
                if name not in used_names:
                    used_names.add(name)
                    break
                attempts += 1

            # Weighted country selection
            c_rand = self.rng.random()
            cum_prob = 0.0
            country_name, _, min_lt, max_lt = countries[0]
            for c_name, prob, c_min, c_max in countries:
                cum_prob += prob
                if c_rand <= cum_prob:
                    country_name, min_lt, max_lt = c_name, c_min, c_max
                    break

            # Clean email
            clean_domain = name.lower().replace(" ", "").replace("&", "").replace(".", "")[:12] + ".com"
            contact_email = f"supply@{clean_domain}"

            # Reliability score between 0.65 and 0.99
            reliability_score = round(self.rng.uniform(0.68, 0.98), 3)
            lead_time_days = self.rng.randint(min_lt, max_lt)

            self.suppliers.append({
                "supplier_id": supplier_id,
                "supplier_name": name,
                "contact_email": contact_email,
                "country": country_name,
                "reliability_score": reliability_score,
                "lead_time_days": lead_time_days,
            })

        return self.suppliers

    def generate_warehouses(self) -> List[Dict[str, Any]]:
        """Generates realistic regional distribution centers."""
        warehouse_templates = [
            ("WH001", "Dallas Central Distribution Center", "Dallas", "TX", "USA", 250000),
            ("WH002", "Chicago Regional Logistics Hub", "Chicago", "IL", "USA", 180000),
            ("WH003", "Seattle Pacific Fulfillment Hub", "Seattle", "WA", "USA", 140000),
            ("WH004", "Atlanta Southeast Mega Hub", "Atlanta", "GA", "USA", 220000),
            ("WH005", "New Jersey Port Gateway Center", "Edison", "NJ", "USA", 195000),
            ("WH006", "Los Angeles West Coast Terminal", "Long Beach", "CA", "USA", 260000),
        ]

        self.warehouses = []
        for wh_id, name, city, state, country, capacity in warehouse_templates:
            self.warehouses.append({
                "warehouse_id": wh_id,
                "warehouse_name": name,
                "city": city,
                "state": state,
                "country": country,
                "capacity_units": capacity,
            })

        return self.warehouses

    def generate_products(self, count: int = 100) -> List[Dict[str, Any]]:
        """Generates realistic retail product catalog with cost and pricing tiers."""
        catalog_blueprints = [
            ("Electronics", "Computer Peripherals", ["Mechanical Keyboard", "Wireless Mouse", "USB-C Multiport Hub", "4K Webcam", "Laptop Stand", "Monitor Arm"], 15.0, 110.0),
            ("Electronics", "Audio & Wearables", ["Noise Cancelling Headphones", "Bluetooth Earbuds", "Smart Fitness Tracker", "Portable Speaker"], 22.0, 180.0),
            ("Home & Kitchen", "Cookware & Dining", ["Stainless Steel Cookware Set", "Ceramic Chef Knife", "Pour Over Coffee Maker", "Vacuum Insulated Bottle"], 8.0, 75.0),
            ("Home & Kitchen", "Organization", ["Bamboo Drawer Organizer", "Stackable Storage Bins", "Airtight Food Containers"], 5.0, 45.0),
            ("Apparel", "Activewear", ["Moisture Wicking T-Shirt", "Compression Running Shorts", "Thermal Athletic Hoodie", "Trail Running Socks"], 6.0, 55.0),
            ("Office Supplies", "Desk Setup", ["Ergonomic Memory Foam Cushion", "Desk Leather Pad", "Cable Management Tray", "Adjustable Footrest"], 7.0, 60.0),
            ("Sports & Outdoors", "Camping & Fitness", ["Ultralight Sleeping Pad", "Resistance Bands Set", "Yoga Mat Pro", "Trekking Poles"], 9.0, 85.0),
        ]

        self.products = []
        for i in range(1, count + 1):
            product_id = f"PRD{i:04d}"
            cat, subcat, item_names, min_cost, max_cost = self.rng.choice(catalog_blueprints)
            base_item = self.rng.choice(item_names)
            tier = self.rng.choice(["Standard", "Pro", "Elite", "Ultra", "Compact", "Plus"])
            product_name = f"{tier} {base_item}"

            unit_cost = round(self.rng.uniform(min_cost, max_cost), 2)
            # Markup between 30% and 110%
            markup = self.rng.uniform(1.30, 2.10)
            unit_price = round(unit_cost * markup, 2)

            # Reorder point: threshold when new purchase order should be placed
            reorder_point = self.rng.randint(25, 120)
            # Target stock level: full capacity target for that product
            target_stock_level = int(reorder_point * self.rng.uniform(2.5, 4.5))

            self.products.append({
                "product_id": product_id,
                "product_name": product_name,
                "category": cat,
                "subcategory": subcat,
                "unit_cost": unit_cost,
                "unit_price": unit_price,
                "reorder_point": reorder_point,
                "target_stock_level": target_stock_level,
            })

        return self.products

    def generate_inventory(self) -> List[Dict[str, Any]]:
        """Generates inventory snapshots across all warehouses with deliberate stockout cases."""
        if not self.warehouses or not self.products:
            raise ValueError("Warehouses and products must be generated before inventory.")

        self.inventory = []
        inv_counter = 1
        today = date.today()

        for wh in self.warehouses:
            # Each warehouse carries a subset (75% - 95%) of all products
            sample_k = max(1, int(len(self.products) * self.rng.uniform(0.75, 0.95)))
            stocked_products = self.rng.sample(
                self.products,
                k=min(sample_k, len(self.products))
            )

            for prod in stocked_products:
                inv_id = f"INV{inv_counter:05d}"
                inv_counter += 1

                reorder_pt = prod["reorder_point"]
                target_stock = prod["target_stock_level"]

                # Intentional distribution of inventory health:
                # 8% Out of stock (0 units) -> Stockout analytics test case
                # 17% Low stock (<= reorder_point) -> Reorder trigger analytics
                # 70% Healthy stock (between reorder_point and target_stock)
                # 5% Overstock (> target_stock)
                stock_scenario = self.rng.random()

                if stock_scenario < 0.08:
                    # Stockout
                    stock_on_hand = 0
                    stock_allocated = 0
                    stock_reserved = 0
                elif stock_scenario < 0.25:
                    # Low stock (critical reorder needed)
                    stock_on_hand = self.rng.randint(1, reorder_pt)
                    stock_allocated = self.rng.randint(0, min(5, stock_on_hand))
                    stock_reserved = self.rng.randint(0, min(3, stock_on_hand - stock_allocated))
                elif stock_scenario < 0.95:
                    # Normal / healthy stock
                    stock_on_hand = self.rng.randint(reorder_pt + 5, target_stock)
                    max_alloc = int(stock_on_hand * 0.3)
                    stock_allocated = self.rng.randint(0, max_alloc)
                    stock_reserved = self.rng.randint(0, int(stock_on_hand * 0.1))
                else:
                    # Overstock
                    stock_on_hand = self.rng.randint(target_stock + 1, int(target_stock * 1.5))
                    stock_allocated = self.rng.randint(0, int(stock_on_hand * 0.15))
                    stock_reserved = self.rng.randint(0, int(stock_on_hand * 0.05))

                # Count date within the last 45 days
                last_counted_offset = self.rng.randint(1, 45)
                last_counted_date = (today - timedelta(days=last_counted_offset)).isoformat()

                self.inventory.append({
                    "inventory_id": inv_id,
                    "warehouse_id": wh["warehouse_id"],
                    "product_id": prod["product_id"],
                    "stock_on_hand": stock_on_hand,
                    "stock_allocated": stock_allocated,
                    "stock_reserved": stock_reserved,
                    "last_counted_date": last_counted_date,
                })

        return self.inventory

    def generate_purchase_orders(
        self,
        order_count: int = 600,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Generates purchase orders and line items over a realistic historical timeline."""
        if not self.suppliers or not self.warehouses or not self.products:
            raise ValueError("Suppliers, warehouses, and products must exist before generating POs.")

        end = end_date or date.today()
        start = start_date or (end - timedelta(days=365))
        total_days = (end - start).days

        supplier_lookup = {s["supplier_id"]: s for s in self.suppliers}

        self.purchase_orders = []
        self.purchase_order_items = []
        item_counter = 1

        for i in range(1, order_count + 1):
            order_id = f"PO{i:05d}"
            supplier = self.rng.choice(self.suppliers)
            warehouse = self.rng.choice(self.warehouses)

            # Random order date in timeframe
            order_day_offset = self.rng.randint(0, total_days)
            order_date = start + timedelta(days=order_day_offset)

            nominal_lead_time = supplier["lead_time_days"]
            expected_delivery = order_date + timedelta(days=nominal_lead_time)

            days_since_order = (end - order_date).days

            # Determine order status based on age and supplier reliability
            # If the order is very recent (< 7 days), it's likely PENDING
            if days_since_order < nominal_lead_time // 2:
                status = "PENDING"
                actual_delivery = None
            elif days_since_order < nominal_lead_time and self.rng.random() < 0.60:
                status = "PENDING"
                actual_delivery = None
            else:
                # Historical order: resolve status
                roll = self.rng.random()
                rel_score = supplier["reliability_score"]

                if roll < 0.03:
                    # Rare cancellation
                    status = "CANCELLED"
                    actual_delivery = None
                elif roll < 0.08:
                    # Partial shipment
                    status = "PARTIALLY_RECEIVED"
                    actual_delivery = expected_delivery + timedelta(days=self.rng.randint(-2, 7))
                else:
                    status = "RECEIVED"
                    # Delivery timing: high reliability suppliers deliver on time, lower have delays
                    if self.rng.random() <= rel_score:
                        # On-time or early (-3 to 0 days delta from expected)
                        actual_delivery = expected_delivery + timedelta(days=self.rng.randint(-3, 0))
                    else:
                        # Late delivery (1 to 14 days late) -> Key for supplier lead time analytics!
                        delay_days = self.rng.randint(1, 14)
                        actual_delivery = expected_delivery + timedelta(days=delay_days)

            # If actual delivery falls in the future relative to 'end', the order has not arrived yet
            if actual_delivery and actual_delivery > end:
                status = "PENDING"
                actual_delivery = None

            # Generate 1 to 6 items per purchase order
            num_items = self.rng.randint(1, 6)
            ordered_products = self.rng.sample(self.products, k=min(num_items, len(self.products)))

            order_total = 0.0

            for prod in ordered_products:
                order_item_id = f"POI{item_counter:06d}"
                item_counter += 1

                qty_ordered = self.rng.randint(15, 250)

                # Determine received quantity based on status
                if status == "RECEIVED":
                    qty_received = qty_ordered
                elif status == "PARTIALLY_RECEIVED":
                    qty_received = int(qty_ordered * self.rng.uniform(0.35, 0.85))
                else:  # PENDING or CANCELLED
                    qty_received = 0

                # Negotiated wholesale price (close to product unit_cost +- 5%)
                po_unit_price = round(prod["unit_cost"] * self.rng.uniform(0.95, 1.05), 2)
                line_total = round(qty_ordered * po_unit_price, 2)
                order_total += line_total

                self.purchase_order_items.append({
                    "order_item_id": order_item_id,
                    "order_id": order_id,
                    "product_id": prod["product_id"],
                    "quantity_ordered": qty_ordered,
                    "quantity_received": qty_received,
                    "unit_price": po_unit_price,
                    "line_total": line_total,
                })

            self.purchase_orders.append({
                "order_id": order_id,
                "supplier_id": supplier["supplier_id"],
                "warehouse_id": warehouse["warehouse_id"],
                "order_date": order_date.isoformat(),
                "expected_delivery_date": expected_delivery.isoformat(),
                "actual_delivery_date": actual_delivery.isoformat() if actual_delivery else "",
                "order_status": status,
                "total_amount": round(order_total, 2),
            })

        return self.purchase_orders, self.purchase_order_items

    def save_dataset(self, filename: str, records: List[Dict[str, Any]]) -> Tuple[Path, Path]:
        """Saves records to CSV in both data/raw/ and a sample in data/sample/."""
        if not records:
            raise ValueError(f"No records to save for {filename}")

        raw_path = self.output_dir / filename
        sample_path = self.sample_dir / filename

        fieldnames = list(records[0].keys())

        # Write complete raw CSV
        with open(raw_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        # Write top 5 sample rows for quick GitHub preview
        with open(sample_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records[:5])

        return raw_path, sample_path

    def run_all(
        self,
        num_suppliers: int = 25,
        num_products: int = 100,
        num_orders: int = 600
    ) -> Dict[str, int]:
        """Executes full pipeline and writes all datasets."""
        self.generate_suppliers(count=num_suppliers)
        self.generate_warehouses()
        self.generate_products(count=num_products)
        self.generate_inventory()
        self.generate_purchase_orders(order_count=num_orders)

        datasets = [
            ("suppliers.csv", self.suppliers),
            ("warehouses.csv", self.warehouses),
            ("products.csv", self.products),
            ("inventory.csv", self.inventory),
            ("purchase_orders.csv", self.purchase_orders),
            ("purchase_order_items.csv", self.purchase_order_items),
        ]

        summary = {}
        for fname, data in datasets:
            self.save_dataset(fname, data)
            summary[fname] = len(data)

        return summary


def main():
    parser = argparse.ArgumentParser(description="RetailFlow Synthetic Data Generator")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--suppliers", type=int, default=25, help="Number of suppliers")
    parser.add_argument("--products", type=int, default=100, help="Number of products")
    parser.add_argument("--orders", type=int, default=600, help="Number of purchase orders")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "raw"),
        help="Target output directory"
    )
    parser.add_argument(
        "--sample-dir",
        type=str,
        default=str(PROJECT_ROOT / "data" / "sample"),
        help="Target sample directory"
    )

    args = parser.parse_args()

    print(f"Generating synthetic retail supply chain data (seed={args.seed})...")
    generator = RetailDataGenerator(
        seed=args.seed,
        output_dir=args.output_dir,
        sample_dir=args.sample_dir
    )
    summary = generator.run_all(
        num_suppliers=args.suppliers,
        num_products=args.products,
        num_orders=args.orders
    )

    print("\nGeneration Complete! Summary of generated records:")
    for filename, count in summary.items():
        print(f"  - {filename:<28}: {count:>5} records")


if __name__ == "__main__":
    main()
