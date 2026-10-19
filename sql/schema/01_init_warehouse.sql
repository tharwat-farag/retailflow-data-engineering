-- =============================================================================
-- RetailFlow Data Warehouse: Schema Initialization & Star Schema Definition
-- =============================================================================
-- Medallion Architecture:
-- 1. 'staging' schema: Operational/conformed Silver mirrors.
-- 2. 'analytics' schema: Star schema (Facts & Dimensions) for analytical queries.
-- =============================================================================

-- Create isolated schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================================================
-- 1. STAGING TABLES (Mirrors of validated Silver datasets)
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    reliability_score NUMERIC(5, 3) NOT NULL,
    lead_time_days INT NOT NULL,
    reliability_tier VARCHAR(50) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_warehouses (
    warehouse_id VARCHAR(20) PRIMARY KEY,
    warehouse_name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(10) NOT NULL,
    country VARCHAR(100) NOT NULL,
    capacity_units INT NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100) NOT NULL,
    unit_cost NUMERIC(10, 2) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    margin_amount NUMERIC(10, 2) NOT NULL,
    margin_percentage NUMERIC(5, 2) NOT NULL,
    reorder_point INT NOT NULL,
    target_stock_level INT NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_inventory (
    inventory_id VARCHAR(30) PRIMARY KEY,
    warehouse_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    stock_on_hand INT NOT NULL,
    stock_allocated INT NOT NULL,
    stock_reserved INT NOT NULL,
    stock_available INT NOT NULL,
    inventory_status VARCHAR(50) NOT NULL,
    last_counted_date DATE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_purchase_orders (
    order_id VARCHAR(30) PRIMARY KEY,
    supplier_id VARCHAR(20) NOT NULL,
    warehouse_id VARCHAR(20) NOT NULL,
    order_date DATE NOT NULL,
    expected_delivery_date DATE NOT NULL,
    actual_delivery_date DATE,
    order_status VARCHAR(50) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    delivery_delay_days INT,
    is_delayed BOOLEAN,
    fulfillment_lead_time_days INT,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.stg_purchase_order_items (
    order_item_id VARCHAR(30) PRIMARY KEY,
    order_id VARCHAR(30) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    quantity_ordered INT NOT NULL,
    quantity_received INT NOT NULL,
    quantity_pending INT NOT NULL,
    fulfillment_ratio NUMERIC(6, 4) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    line_total NUMERIC(12, 2) NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 2. ANALYTICAL STAR SCHEMA DIMENSIONS
-- =============================================================================

-- Date Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INT PRIMARY KEY,                 -- YYYYMMDD (e.g. 20261018)
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Supplier Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_supplier (
    supplier_key SERIAL PRIMARY KEY,
    supplier_id VARCHAR(20) NOT NULL UNIQUE,  -- Natural Key
    supplier_name VARCHAR(255) NOT NULL,
    contact_email VARCHAR(255) NOT NULL,
    country VARCHAR(100) NOT NULL,
    reliability_score NUMERIC(5, 3) NOT NULL,
    lead_time_days INT NOT NULL,
    reliability_tier VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Warehouse Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_warehouse (
    warehouse_key SERIAL PRIMARY KEY,
    warehouse_id VARCHAR(20) NOT NULL UNIQUE, -- Natural Key
    warehouse_name VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(10) NOT NULL,
    country VARCHAR(100) NOT NULL,
    capacity_units INT NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Product Dimension
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(20) NOT NULL UNIQUE,   -- Natural Key
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100) NOT NULL,
    unit_cost NUMERIC(10, 2) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    margin_amount NUMERIC(10, 2) NOT NULL,
    margin_percentage NUMERIC(5, 2) NOT NULL,
    reorder_point INT NOT NULL,
    target_stock_level INT NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 3. ANALYTICAL STAR SCHEMA FACTS
-- =============================================================================

-- Fact 1: Periodic Inventory Snapshot
CREATE TABLE IF NOT EXISTS analytics.fact_inventory_snapshot (
    inventory_snapshot_key SERIAL PRIMARY KEY,
    inventory_id VARCHAR(30) NOT NULL,
    date_key INT NOT NULL REFERENCES analytics.dim_date(date_key),
    warehouse_key INT NOT NULL REFERENCES analytics.dim_warehouse(warehouse_key),
    product_key INT NOT NULL REFERENCES analytics.dim_product(product_key),
    stock_on_hand INT NOT NULL CHECK (stock_on_hand >= 0),
    stock_allocated INT NOT NULL CHECK (stock_allocated >= 0),
    stock_reserved INT NOT NULL CHECK (stock_reserved >= 0),
    stock_available INT NOT NULL CHECK (stock_available >= 0),
    inventory_status VARCHAR(50) NOT NULL,
    last_counted_date DATE NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_inventory UNIQUE (inventory_id, date_key)
);

-- Fact 2: Purchase Order Line Item Fulfillment
CREATE TABLE IF NOT EXISTS analytics.fact_purchase_order_fulfillment (
    fulfillment_key SERIAL PRIMARY KEY,
    order_item_id VARCHAR(30) NOT NULL UNIQUE,
    order_id VARCHAR(30) NOT NULL,
    order_date_key INT NOT NULL REFERENCES analytics.dim_date(date_key),
    expected_delivery_date_key INT NOT NULL REFERENCES analytics.dim_date(date_key),
    actual_delivery_date_key INT REFERENCES analytics.dim_date(date_key), -- Null for pending/cancelled
    supplier_key INT NOT NULL REFERENCES analytics.dim_supplier(supplier_key),
    warehouse_key INT NOT NULL REFERENCES analytics.dim_warehouse(warehouse_key),
    product_key INT NOT NULL REFERENCES analytics.dim_product(product_key),
    order_status VARCHAR(50) NOT NULL,
    quantity_ordered INT NOT NULL CHECK (quantity_ordered > 0),
    quantity_received INT NOT NULL CHECK (quantity_received >= 0),
    quantity_pending INT NOT NULL CHECK (quantity_pending >= 0),
    fulfillment_ratio NUMERIC(6, 4) NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    line_total NUMERIC(12, 2) NOT NULL,
    delivery_delay_days INT,
    is_delayed BOOLEAN,
    fulfillment_lead_time_days INT,
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
