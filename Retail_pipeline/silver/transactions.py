from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_text(name: str):
    """Trim a string column and normalize empty / whitespace-only values to NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed != "", trimmed)


@dp.table(
    name="transactions",
    comment=(
        "Silver transactions streaming table (no SCD history, not a materialized view). "
        "Streams retail.bronze.transactions, where every column arrives as STRING: IDs "
        "and text fields are trimmed with empty strings normalized to NULL (source has "
        "trailing spaces in opportunity_name and payment_mode), quantity is cast to INT, "
        "prices to DECIMAL(12,2), and the Oracle-style transaction timestamp "
        "(dd-MMM-yyyy hh.mm.ss a) is parsed to TIMESTAMP. net_amount is derived as "
        "selling_price - discount_amount, and the empty _rescued_data column is "
        "dropped. Rows missing transaction_id fail the update; rows missing keys or "
        "with negative quantity / prices, or an unparseable timestamp are dropped. "
        "opportunity_name joins to silver.opportunity.opportunity_name; product_id and "
        "store_id join to product_catalog and inventory."
    ),
    cluster_by=["store_id"],
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "data_quality": "silver",
    },
)
@dp.expect_or_fail("transaction_id is present", "transaction_id IS NOT NULL")
@dp.expect_or_drop("product_id is populated", "product_id IS NOT NULL")
@dp.expect_or_drop("store_id is populated", "store_id IS NOT NULL")
@dp.expect_or_drop(
    "quantity is a non-negative integer",
    "quantity IS NOT NULL AND quantity >= 0",
)
@dp.expect_or_drop(
    "selling_price is non-negative",
    "selling_price IS NOT NULL AND selling_price >= 0",
)
@dp.expect_or_drop(
    "discount_amount is non-negative",
    "discount_amount IS NOT NULL AND discount_amount >= 0",
)
@dp.expect_or_drop(
    "transaction_timestamp is parseable",
    "transaction_timestamp IS NOT NULL",
)
def transactions():
    # Bronze transactions is a plain Delta table ingested once; a plain streaming
    # read processes new commits incrementally, no skipChangeCommits needed.
    bronze = spark.readStream.table("retail.bronze.transactions")

    return bronze.select(
        # Identity
        clean_text("transaction_id").alias("transaction_id"),
        clean_text("opportunity_name").alias("opportunity_name"),
        clean_text("product_id").alias("product_id"),
        clean_text("store_id").alias("store_id"),
        # Amounts (source columns are all STRING)
        F.col("quantity").cast("INT").alias("quantity"),
        F.col("selling_price").cast("DECIMAL(12,2)").alias("selling_price"),
        F.col("discount_amount").cast("DECIMAL(12,2)").alias("discount_amount"),
        # Oracle-style timestamp, e.g. '17-Mar-2026 05.09.51 PM'
        F.to_timestamp("transaction_timestamp", "dd-MMM-yyyy hh.mm.ss a").alias(
            "transaction_timestamp"
        ),
        # Classification
        clean_text("payment_mode").alias("payment_mode"),
        clean_text("sales_channel").alias("sales_channel"),
        # Derived
        (F.col("selling_price") - F.col("discount_amount"))
        .cast("DECIMAL(12,2)")
        .alias("net_amount"),
        # Processing time: when this row was written to silver
        F.current_timestamp().alias("updated_at"),
        # _rescued_data from bronze is empty for all rows and is intentionally dropped.
    )
