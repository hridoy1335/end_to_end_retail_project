-- Unity Catalog metric view for retail KPIs at transaction grain.
-- Created 2026-11-09 at retail.retail_semantic.retail_metrics and validated
-- (monthly Net Revenue / Transaction Count / AOV cross-checked against direct source queries).
-- Re-run this statement to redefine the metric view (CREATE OR REPLACE).

CREATE OR REPLACE VIEW retail.retail_semantic.retail_metrics WITH METRICS LANGUAGE YAML AS
$$
version: 1.1
comment: "Retail sales KPIs at transaction grain. Source: retail.gold.fact_sales joined to dim_product (point-in-time SCD2 surrogate key product_sk) and dim_calendar (date_key). Customer dimension is not joinable: fact_sales carries no customer key."
source: retail.gold.fact_sales

joins:
  - name: product
    source: retail.gold.dim_product
    on: source.product_sk = product.product_sk
    rely:
      at_most_one_match: true
  - name: calendar
    source: retail.gold.dim_calendar
    on: source.date_key = calendar.date_key
    rely:
      at_most_one_match: true

fields:
  - name: Transaction Date
    expr: source.transaction_date
    comment: "Calendar date of the sale"
  - name: Year
    expr: calendar.year
  - name: Quarter
    expr: concat(calendar.year, '-Q', calendar.quarter)
    comment: "e.g. 2026-Q1"
  - name: Month
    expr: calendar.year_month
    comment: "e.g. 2026-03"
  - name: Week of Year
    expr: calendar.week_of_year
  - name: Day of Week
    expr: calendar.day_of_week_name
  - name: Weekend
    expr: calendar.is_weekend
  - name: Product Name
    expr: product.product_name
  - name: Category
    expr: source.category
  - name: Subcategory
    expr: source.subcategory
  - name: Brand
    expr: source.brand
  - name: Supplier
    expr: product.supplier_name
  - name: Store
    expr: source.store_id
  - name: Payment Mode
    expr: source.payment_mode
  - name: Sales Channel
    expr: source.sales_channel
  - name: Opportunity Stage
    expr: source.stage_name

measures:
  - name: Transaction Count
    expr: COUNT(1)
    comment: "Number of sales transactions"
  - name: Total Units Sold
    expr: SUM(source.quantity)
    comment: "Sum of units sold"
  - name: Gross Sales
    expr: SUM(source.selling_price)
    comment: "Sum of selling price before discount"
  - name: Total Discount
    expr: SUM(source.discount_amount)
    comment: "Sum of discounts applied"
  - name: Net Revenue
    expr: SUM(source.net_amount)
    comment: "Selling price minus discount, summed"
  - name: Average Order Value
    expr: AVG(source.net_amount)
    comment: "Average net amount per transaction"
  - name: Discount Rate
    expr: SUM(source.discount_amount) / NULLIF(SUM(source.selling_price), 0)
    comment: "Discount as a share of gross sales"
  - name: Won Transaction Count
    expr: COUNT(1) FILTER (WHERE source.is_won)
    comment: "Transactions tied to won opportunities"
  - name: Distinct Products Sold
    expr: COUNT(DISTINCT source.product_id)
    comment: "Number of distinct products sold"
$$;

-- Validation example (update the target to the confirmed catalog.schema):
-- SELECT `Month`, MEASURE(`Net Revenue`) AS net_revenue, MEASURE(`Transaction Count`) AS txn_count
-- FROM retail.retail_semantic.retail_metrics
-- GROUP BY `Month` ORDER BY `Month`;
