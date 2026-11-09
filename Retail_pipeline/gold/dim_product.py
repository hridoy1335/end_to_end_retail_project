from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_text(name: str):
    """Trim a string column and normalize empty / whitespace-only values to NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed != "", trimmed)


@dp.materialized_view(
    name="retail.gold.dim_product",
    comment=(
        "Gold product dimension with SCD Type 2 history (surrogate key product_sk, "
        "validity window valid_from / valid_to, is_current flag). Sourced from "
        "retail.bronze.product_catalog because the bronze Auto CDC target is the only "
        "layer that retains version history (e.g. iPhone 15 / Galaxy S24 price changes "
        "on 2026-11-08); silver intentionally keeps only the latest row per product. "
        "Point-in-time lookup: valid_from <= t AND (valid_to > t OR valid_to IS "
        "NULL). Materialized view so fact tables can batch-join the SCD windows."
    ),
    cluster_by=["product_id"],
    table_properties={"data_quality": "gold"},
)
@dp.expect_or_fail("product_id is present", "product_id IS NOT NULL")
@dp.expect_or_fail("valid_from is present", "valid_from IS NOT NULL")
@dp.expect("closed versions have a valid_to", "valid_to IS NULL OR valid_to > valid_from")
def dim_product():
    bronze = spark.read.table("retail.bronze.product_catalog")

    return bronze.select(
        # Surrogate key: stable per product version
        F.md5(F.concat_ws("|", clean_text("product_id"), F.col("__START_AT").cast("STRING")))
        .alias("product_sk"),
        # Natural key and descriptive attributes
        clean_text("product_id").alias("product_id"),
        clean_text("product_name").alias("product_name"),
        clean_text("category").alias("category"),
        clean_text("subcategory").alias("subcategory"),
        clean_text("brand").alias("brand"),
        F.col("unit_price").cast("DECIMAL(12,2)").alias("unit_price"),
        clean_text("supplier_name").alias("supplier_name"),
        F.col("launch_date").cast("DATE").alias("launch_date"),
        F.coalesce(F.col("is_active"), F.lit(False)).alias("is_active"),
        # SCD Type 2 validity window (bronze __START_AT / __END_AT)
        F.col("__START_AT").alias("valid_from"),
        F.col("__END_AT").alias("valid_to"),
        F.col("__END_AT").isNull().alias("is_current"),
    )
