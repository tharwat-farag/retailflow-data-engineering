-- =============================================================================
-- RetailFlow Data Warehouse: Performance Optimization Indexes
-- =============================================================================

-- Staging lookup indexes
CREATE INDEX IF NOT EXISTS idx_stg_inv_wh ON staging.stg_inventory(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_stg_inv_prod ON staging.stg_inventory(product_id);
CREATE INDEX IF NOT EXISTS idx_stg_po_sup ON staging.stg_purchase_orders(supplier_id);
CREATE INDEX IF NOT EXISTS idx_stg_po_wh ON staging.stg_purchase_orders(warehouse_id);
CREATE INDEX IF NOT EXISTS idx_stg_poi_order ON staging.stg_purchase_order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_stg_poi_prod ON staging.stg_purchase_order_items(product_id);

-- Star Schema Dimension Indexes
CREATE INDEX IF NOT EXISTS idx_dim_prod_cat ON analytics.dim_product(category, subcategory);
CREATE INDEX IF NOT EXISTS idx_dim_sup_country ON analytics.dim_supplier(country);
CREATE INDEX IF NOT EXISTS idx_dim_sup_tier ON analytics.dim_supplier(reliability_tier);
CREATE INDEX IF NOT EXISTS idx_dim_wh_state ON analytics.dim_warehouse(state);
CREATE INDEX IF NOT EXISTS idx_dim_date_year_month ON analytics.dim_date(year, month);

-- Star Schema Fact Indexes: Inventory Snapshots
CREATE INDEX IF NOT EXISTS idx_fact_inv_date_key ON analytics.fact_inventory_snapshot(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_inv_wh_key ON analytics.fact_inventory_snapshot(warehouse_key);
CREATE INDEX IF NOT EXISTS idx_fact_inv_prod_key ON analytics.fact_inventory_snapshot(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_inv_status ON analytics.fact_inventory_snapshot(inventory_status);

-- Star Schema Fact Indexes: Purchase Order Fulfillment
CREATE INDEX IF NOT EXISTS idx_fact_po_order_date ON analytics.fact_purchase_order_fulfillment(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_po_exp_date ON analytics.fact_purchase_order_fulfillment(expected_delivery_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_po_act_date ON analytics.fact_purchase_order_fulfillment(actual_delivery_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_po_sup_key ON analytics.fact_purchase_order_fulfillment(supplier_key);
CREATE INDEX IF NOT EXISTS idx_fact_po_wh_key ON analytics.fact_purchase_order_fulfillment(warehouse_key);
CREATE INDEX IF NOT EXISTS idx_fact_po_prod_key ON analytics.fact_purchase_order_fulfillment(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_po_status ON analytics.fact_purchase_order_fulfillment(order_status);
CREATE INDEX IF NOT EXISTS idx_fact_po_delayed ON analytics.fact_purchase_order_fulfillment(is_delayed);
