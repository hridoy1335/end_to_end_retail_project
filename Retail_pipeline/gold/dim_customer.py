from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="retail.gold.dim_customer",
    comment=(
        "Gold customer dimension (SCD Type 1: latest state only, no history). Sourced "
        "from retail.silver.accounts, which already keeps one row per account with "
        "text fields cleaned. Exposes the attributes analysts filter and group by: "
        "type, industry, billing geography, and the is_buyer flag. Materialized view "
        "for direct SQL/BI consumption."
    ),
    cluster_by=["customer_id"],
    table_properties={"data_quality": "gold"},
)
@dp.expect_or_fail("customer_id is present", "customer_id IS NOT NULL")
def dim_customer():
    accounts = spark.read.table("retail.silver.accounts")

    return accounts.select(
        F.col("account_id").alias("customer_id"),
        F.col("account_name").alias("customer_name"),
        F.col("account_type").alias("customer_type"),
        F.col("industry").alias("industry"),
        F.col("phone").alias("phone"),
        F.col("website").alias("website"),
        F.col("billing_city").alias("billing_city"),
        F.col("billing_state").alias("billing_state"),
        F.col("billing_country").alias("billing_country"),
        F.col("is_buyer").alias("is_buyer"),
        F.col("created_date").alias("customer_since"),
    )
