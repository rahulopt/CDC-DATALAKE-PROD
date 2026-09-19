-- ===========================================================================
-- Athena — query the Gold Iceberg tables (requirement 30 query layer).
--
-- Silver/Gold tables are registered in the Glue Data Catalog by the Glue jobs,
-- so Athena (engine v3, which supports Iceberg) can query them directly. Set
-- the workgroup's query result location to an S3 path first.
--
-- Replace <db> with the Glue database (terraform output `glue_database`,
-- e.g. cdc_lake_dev_silver).
-- ===========================================================================

-- Daily sales trend
SELECT date, total_orders, delivered_orders, cancelled_orders,
       total_revenue, average_order_value
FROM   "<db>"."gold_daily_sales"
ORDER  BY date DESC
LIMIT  30;

-- Top customers by lifetime value
SELECT customer_id, total_orders, lifetime_value, last_order_date
FROM   "<db>"."gold_customer_metrics"
ORDER  BY lifetime_value DESC
LIMIT  20;

-- Best-selling products
SELECT product_id, units_sold, orders_containing
FROM   "<db>"."gold_product_metrics"
ORDER  BY units_sold DESC
LIMIT  20;

-- Order status distribution over time
SELECT date, status, order_count
FROM   "<db>"."gold_order_status"
ORDER  BY date DESC, status;

-- Current-state Silver check (Iceberg time-travel example):
-- SELECT * FROM "<db>"."orders" FOR TIMESTAMP AS OF TIMESTAMP '2026-09-19 18:00:00';
