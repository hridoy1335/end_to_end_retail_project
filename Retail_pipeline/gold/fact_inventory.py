from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="retail.gold.fact_inventory",
    comment=(
        "Gold inventory fact at product-store grain (latest stock snapshot per "
        "inventory record). Built from retail.silver.inventory and enriched with "
        "point-in-time product attributes from dim_product (SCD Type 2 window "
        "covering last_stock_update): category / subcategory / brand and the unit "
        "price that was in effect when stock was last updated, which drives the "
        "derived stock_value = stock_quantity * unit_price_at_snapshot. Unresolved "
        "product attributes are logged as warnings, not dropped."
    ),
    cluster_by=["store_id"],
    table_properties={"data_quality": "gold"},
)
@dp.expect_or_fail("inventory_id is present", "inventory_id IS NOT NULL")
@dp.expect("product attributes resolved", "product_sk IS NOT NULL")
def fact_inventory():
    inv = spark.read.table("retail.silver.inventory")
    dim = spark.read.table("retail.gold.dim_product")

    # Point-in-time join helper: the first version of each product gets an
    # open-start window so stock updates before the first valid_from still match.
    first_version = Window.partitionBy("product_id")
    dim_pit = (
        dim.withColumn("first_from", F.min("valid_from").over(first_version))
        .withColumn(
            "eff_from",
            F.when(
                F.col("valid_from") == F.col("first_from"),
                F.lit("1900-01-01 00:00:00").cast("TIMESTAMP"),
            ).otherwise(F.col("valid_from")),
        )
        .drop("first_from")
    )

    joined = inv.alias("i").join(
        dim_pit.alias("d"),
        (F.col("i.product_id") == F.col("d.product_id"))
        & (F.col("d.eff_from") <= F.col("i.last_stock_update"))
        & (F.col("d.valid_to").isNull() | (F.col("i.last_stock_update") < F.col("d.valid_to"))),
        "left",
    )

    return joined.select(
        # Degenerate dimensions
        F.col("i.inventory_id").alias("inventory_id"),
        # Dimension keys and point-in-time product attributes
        F.col("d.product_sk").alias("product_sk"),
        F.col("i.product_id").alias("product_id"),
        F.col("d.category").alias("category"),
        F.col("d.subcategory").alias("subcategory"),
        F.col("d.brand").alias("brand"),
        # Store context
        F.col("i.store_id").alias("store_id"),
        F.col("i.warehouse_location").alias("warehouse_location"),
        # Measures
        F.col("i.stock_quantity").alias("stock_quantity"),
        F.col("i.reorder_level").alias("reorder_level"),
        F.col("i.needs_restock").alias("needs_restock"),
        F.col("d.unit_price").alias("unit_price_at_snapshot"),
        (F.col("i.stock_quantity") * F.col("d.unit_price"))
        .cast("DECIMAL(18,2)")
        .alias("stock_value"),
        # Audit
        F.col("i.last_stock_update").alias("last_stock_update"),
    )
