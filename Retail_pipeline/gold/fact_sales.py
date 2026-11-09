from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    name="retail.gold.fact_sales",
    comment=(
        "Gold sales fact at transaction grain (one row per transaction). Built from "
        "retail.silver.transactions and enriched with: point-in-time product "
        "attributes from dim_product (SCD Type 2 window covering "
        "transaction_timestamp, so the category / brand / unit price reflect the "
        "product as it was at the moment of sale), the calendar date_key from "
        "dim_calendar, and opportunity stage / won flag from silver.opportunity. The "
        "earliest product version's window is extended open-start because some "
        "transactions (from 2026-01-01 09:48) predate the first version's valid_from "
        "(10:00). ~31 transactions have no matching opportunity name; those keep NULL "
        "stage attributes. Unresolved product or opportunity attributes are logged "
        "as warnings, not dropped."
    ),
    cluster_by=["transaction_date"],
    table_properties={"data_quality": "gold"},
)
@dp.expect_or_fail("transaction_id is present", "transaction_id IS NOT NULL")
@dp.expect("product attributes resolved", "product_sk IS NOT NULL")
@dp.expect("calendar date resolved", "date_key IS NOT NULL")
@dp.expect("opportunity attributes resolved", "stage_name IS NOT NULL")
def fact_sales():
    tx = spark.read.table("retail.silver.transactions")
    dim = spark.read.table("retail.gold.dim_product")
    cal = spark.read.table("retail.gold.dim_calendar")
    opp = spark.read.table("retail.silver.opportunity")

    # Point-in-time join helper: the first version of each product gets an
    # open-start window so transactions before the first valid_from still match.
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

    joined = (
        tx.alias("t")
        .join(
            dim_pit.alias("d"),
            (F.col("t.product_id") == F.col("d.product_id"))
            & (F.col("d.eff_from") <= F.col("t.transaction_timestamp"))
            & (
                F.col("d.valid_to").isNull()
                | (F.col("t.transaction_timestamp") < F.col("d.valid_to"))
            ),
            "left",
        )
        .join(
            opp.alias("o"),
            F.col("t.opportunity_name") == F.col("o.opportunity_name"),
            "left",
        )
        .join(
            cal.alias("c"),
            F.col("c.full_date") == F.to_date(F.col("t.transaction_timestamp")),
            "left",
        )
    )

    return joined.select(
        # Degenerate dimensions
        F.col("t.transaction_id").alias("transaction_id"),
        F.col("t.transaction_timestamp").alias("transaction_timestamp"),
        F.to_date(F.col("t.transaction_timestamp")).alias("transaction_date"),
        F.col("c.date_key").alias("date_key"),
        # Dimension keys and point-in-time product attributes
        F.col("d.product_sk").alias("product_sk"),
        F.col("t.product_id").alias("product_id"),
        F.col("d.category").alias("category"),
        F.col("d.subcategory").alias("subcategory"),
        F.col("d.brand").alias("brand"),
        F.col("d.unit_price").alias("unit_price_at_sale"),
        # Opportunity attributes
        F.col("t.opportunity_name").alias("opportunity_name"),
        F.col("o.stage_name").alias("stage_name"),
        F.col("o.is_won").alias("is_won"),
        # Store / channel context
        F.col("t.store_id").alias("store_id"),
        F.col("t.payment_mode").alias("payment_mode"),
        F.col("t.sales_channel").alias("sales_channel"),
        # Measures
        F.col("t.quantity").alias("quantity"),
        F.col("t.selling_price").alias("selling_price"),
        F.col("t.discount_amount").alias("discount_amount"),
        F.col("t.net_amount").alias("net_amount"),
    )
