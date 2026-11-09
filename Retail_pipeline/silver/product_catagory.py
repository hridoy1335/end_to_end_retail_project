from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_text(name: str):
    """Trim a string column and normalize empty / whitespace-only values to NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed != "", trimmed)


@dp.table(
    name="product_catalog",
    comment=(
        "Silver product catalog streaming table holding the latest version of each "
        "product (no SCD history, not a materialized view). Streams the current "
        "state of retail.bronze.product_catalog (Auto CDC SCD Type 2) with "
        "skipChangeCommits and atomically replaces each product's previous row on "
        "every new version via a REPLACE USING flow keyed on product_id and "
        "sequenced by source_updated_at. Superseded SCD versions (__END_AT set) are "
        "excluded. Text fields are trimmed with empty strings normalized to NULL, "
        "unit_price is cast to DECIMAL(12,2), launch_date to DATE, and is_active is "
        "normalized to false when absent. Rows missing product_id fail the update; "
        "rows missing name or category, or with a negative unit_price are dropped. "
        "product_id is the join key used by silver.inventory and "
        "silver.transactions."
    ),
    cluster_by=["product_id"],
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "data_quality": "silver",
    },
    replace_using=["product_id"],
    sequence_by="source_updated_at",
)
@dp.expect_or_fail("product_id is present", "product_id IS NOT NULL")
@dp.expect_or_drop("product_name is populated", "product_name IS NOT NULL")
@dp.expect_or_drop("category is populated", "category IS NOT NULL")
@dp.expect_or_drop(
    "unit_price is non-negative", "unit_price IS NOT NULL AND unit_price >= 0"
)
def product_catalog():
    # skipChangeCommits: bronze is an Auto CDC (SCD Type 2) target whose merges
    # rewrite existing rows; ignore those commits and process only newly added rows.
    bronze = spark.readStream.option("skipChangeCommits", "true").table(
        "retail.bronze.product_catalog"
    )

    # Current state of each product, excluding superseded SCD versions. The
    # REPLACE USING flow keeps the latest row per product_id (sequenced by
    # source_updated_at, which carries the bronze __START_AT value).
    current = bronze.where(F.col("__END_AT").isNull())

    return current.select(
        # Identity
        clean_text("product_id").alias("product_id"),
        clean_text("product_name").alias("product_name"),
        # Classification
        clean_text("category").alias("category"),
        clean_text("subcategory").alias("subcategory"),
        clean_text("brand").alias("brand"),
        # Commercial
        F.col("unit_price").cast("DECIMAL(12,2)").alias("unit_price"),
        clean_text("supplier_name").alias("supplier_name"),
        F.col("launch_date").cast("DATE").alias("launch_date"),
        F.coalesce(F.col("is_active"), F.lit(False)).alias("is_active"),
        # Audit: source version timestamp doubles as the REPLACE USING sequence
        F.col("updated_at").alias("source_updated_at"),
        # Processing time: when this row was last written to silver
        F.current_timestamp().alias("updated_at"),
    )
