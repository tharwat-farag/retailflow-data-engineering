-- =============================================================================
-- RetailFlow Data Warehouse: Key Supply Chain Analytical Queries
-- =============================================================================
-- Run these queries in pgAdmin, DBeaver, or psql against 'retailflow_warehouse'
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. INVENTORY STOCKOUT & REORDER RISK ANALYSIS
-- Identifies products currently stocked out (0 on hand) or critically below reorder
-- threshold across each fulfillment center, calculating estimated replenishment cost.
-- -----------------------------------------------------------------------------
SELECT
    w.warehouse_name,
    w.city,
    w.state,
    p.category,
    p.product_id,
    p.product_name,
    f.stock_on_hand,
    f.stock_available,
    p.reorder_point,
    p.target_stock_level,
    (p.target_stock_level - f.stock_on_hand) AS units_needed_to_replenish,
    ROUND((p.target_stock_level - f.stock_on_hand) * p.unit_cost, 2) AS estimated_replenishment_cost_usd,
    f.inventory_status
FROM analytics.fact_inventory_snapshot f
JOIN analytics.dim_warehouse w ON f.warehouse_key = w.warehouse_key
JOIN analytics.dim_product p ON f.product_key = p.product_key
WHERE f.inventory_status IN ('OUT_OF_STOCK', 'LOW_STOCK')
ORDER BY
    CASE f.inventory_status WHEN 'OUT_OF_STOCK' THEN 1 ELSE 2 END,
    estimated_replenishment_cost_usd DESC;

-- -----------------------------------------------------------------------------
-- 2. SUPPLIER DELIVERY PERFORMANCE & ON-TIME RELIABILITY SCORECARD
-- Measures fulfillment metrics: total orders, total spend, on-time delivery rate (OTD),
-- delayed shipments, and average delivery delay by supplier and country.
-- -----------------------------------------------------------------------------
SELECT
    s.supplier_id,
    s.supplier_name,
    s.country,
    s.reliability_tier,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity_ordered) AS total_units_ordered,
    SUM(f.quantity_received) AS total_units_received,
    ROUND(SUM(f.line_total), 2) AS total_procurement_spend_usd,
    COUNT(CASE WHEN f.order_status = 'RECEIVED' AND f.is_delayed = FALSE THEN 1 END) AS on_time_shipments,
    COUNT(CASE WHEN f.order_status = 'RECEIVED' AND f.is_delayed = TRUE THEN 1 END) AS delayed_shipments,
    ROUND(
        COUNT(CASE WHEN f.order_status = 'RECEIVED' AND f.is_delayed = FALSE THEN 1 END)::NUMERIC /
        NULLIF(COUNT(CASE WHEN f.order_status = 'RECEIVED' THEN 1 END), 0) * 100,
        2
    ) AS on_time_delivery_rate_pct,
    ROUND(AVG(CASE WHEN f.is_delayed = TRUE THEN f.delivery_delay_days END), 1) AS avg_delay_days
FROM analytics.fact_purchase_order_fulfillment f
JOIN analytics.dim_supplier s ON f.supplier_key = s.supplier_key
WHERE f.order_status = 'RECEIVED'
GROUP BY
    s.supplier_id,
    s.supplier_name,
    s.country,
    s.reliability_tier
ORDER BY total_procurement_spend_usd DESC;

-- -----------------------------------------------------------------------------
-- 3. DELAYED PURCHASE ORDERS DRILLDOWN
-- Identifies all purchase order lines that arrived after contractual expected date,
-- calculating financial value of delayed goods and impact per fulfillment hub.
-- -----------------------------------------------------------------------------
SELECT
    f.order_id,
    f.order_item_id,
    d_ord.full_date AS order_date,
    d_exp.full_date AS expected_delivery_date,
    d_act.full_date AS actual_delivery_date,
    f.delivery_delay_days,
    s.supplier_name,
    s.country AS supplier_country,
    w.warehouse_name,
    p.product_name,
    f.quantity_ordered,
    f.line_total AS order_line_valuation_usd
FROM analytics.fact_purchase_order_fulfillment f
JOIN analytics.dim_supplier s ON f.supplier_key = s.supplier_key
JOIN analytics.dim_warehouse w ON f.warehouse_key = w.warehouse_key
JOIN analytics.dim_product p ON f.product_key = p.product_key
JOIN analytics.dim_date d_ord ON f.order_date_key = d_ord.date_key
JOIN analytics.dim_date d_exp ON f.expected_delivery_date_key = d_exp.date_key
JOIN analytics.dim_date d_act ON f.actual_delivery_date_key = d_act.date_key
WHERE f.is_delayed = TRUE
ORDER BY f.delivery_delay_days DESC, f.line_total DESC;

-- -----------------------------------------------------------------------------
-- 4. WAREHOUSE STORAGE & UTILIZATION SNAPSHOT
-- Aggregates current physical unit volumes against warehouse max capacity limits
-- to evaluate regional storage pressure.
-- -----------------------------------------------------------------------------
SELECT
    w.warehouse_id,
    w.warehouse_name,
    w.city,
    w.state,
    w.capacity_units,
    SUM(f.stock_on_hand) AS total_physical_stock_on_hand,
    SUM(f.stock_allocated) AS total_stock_allocated,
    SUM(f.stock_reserved) AS total_stock_reserved,
    SUM(f.stock_available) AS total_stock_available_for_sale,
    ROUND((SUM(f.stock_on_hand)::NUMERIC / w.capacity_units) * 100, 2) AS capacity_utilization_pct
FROM analytics.fact_inventory_snapshot f
JOIN analytics.dim_warehouse w ON f.warehouse_key = w.warehouse_key
GROUP BY
    w.warehouse_id,
    w.warehouse_name,
    w.city,
    w.state,
    w.capacity_units
ORDER BY capacity_utilization_pct DESC;

-- -----------------------------------------------------------------------------
-- 5. PRODUCT CATEGORY MARGIN & PROFITABILITY CONTRIBUTION
-- Analyzes sales pricing and profitability contribution across retail departments.
-- -----------------------------------------------------------------------------
SELECT
    p.category,
    COUNT(DISTINCT p.product_id) AS total_catalog_products,
    ROUND(AVG(p.unit_cost), 2) AS avg_unit_cost_usd,
    ROUND(AVG(p.unit_price), 2) AS avg_unit_price_usd,
    ROUND(AVG(p.margin_amount), 2) AS avg_margin_amount_usd,
    ROUND(AVG(p.margin_percentage), 2) AS avg_margin_percentage_pct
FROM analytics.dim_product p
GROUP BY p.category
ORDER BY avg_margin_percentage_pct DESC;
