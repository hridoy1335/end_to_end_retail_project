from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_text(name: str):
    """Trim a string column and normalize empty / whitespace-only values to NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed != "", trimmed)


@dp.table(
    name="inventory",
    comment=(
        "Silver inventory streaming table (no SCD history, not a materialized view). "
        "Streams retail.bronze.inventory, trims text fields with empty strings "
        "normalized to NULL (e.g. trailing whitespace in warehouse_location), casts "
        "quantities to INT, and derives a needs_restock flag (stock at or below the "
        "reorder level). Rows missing keys or with negative quantities are dropped; "
        "a missing inventory_id fails the update; future-dated stock updates are "
        "logged as warnings."
    ),
    cluster_by=["store_id"],
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "data_quality": "silver",
    },
)
@dp.expect_or_fail("inventory_id is present", "inventory_id IS NOT NULL")
@dp.expect_or_drop("product_id is populated", "product_id IS NOT NULL")
@dp.expect_or_drop("store_id is populated", "store_id IS NOT NULL")
@dp.expect_or_drop(
    "stock_quantity is a non-negative integer",
    "stock_quantity IS NOT NULL AND stock_quantity >= 0",
)
@dp.expect_or_drop(
    "reorder_level is a non-negative integer",
    "reorder_level IS NOT NULL AND reorder_level >= 0",
)
@dp.expect(
    "last_stock_update is not in the future",
    "last_stock_update IS NULL OR last_stock_update <= current_timestamp()",
)
def inventory():
    # Bronze inventory is an append-only streaming table: a plain streaming read,
    # no skipChangeCommits needed.
    bronze = spark.readStream.table("retail.bronze.inventory")

    return bronze.select(
        # Identity
        clean_text("inventory_id").alias("inventory_id"),
        clean_text("product_id").alias("product_id"),
        clean_text("store_id").alias("store_id"),
        # Stock levels
        F.col("stock_quantity").cast("INT").alias("stock_quantity"),
        F.col("reorder_level").cast("INT").alias("reorder_level"),
        # Location and audit
        clean_text("warehouse_location").alias("warehouse_location"),
        F.col("last_stock_update").cast("TIMESTAMP").alias("last_stock_update"),
        # Derived: stock at or below the reorder point
        (F.col("stock_quantity") <= F.col("reorder_level"))
        .cast("BOOLEAN")
        .alias("needs_restock"),
        # Processing time: when this row was written to silver
        F.current_timestamp().alias("updated_at"),
    )
