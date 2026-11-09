from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="retail.gold.dim_calendar",
    comment=(
        "Gold calendar dimension covering 2025-01-01 through 2027-12-31 (fixed, "
        "deterministic range: product launch dates start in 2025 and 2027 gives "
        "planning headroom). One row per date with date_key (yyyyMMdd INT) for fact "
        "joins, plus year / quarter / month / week / weekday attributes analysts "
        "group by. Materialized view; it computes once and only changes if the range "
        "definition changes."
    ),
    cluster_by=["full_date"],
    table_properties={"data_quality": "gold"},
)
@dp.expect_or_fail("calendar date is present", "full_date IS NOT NULL")
def dim_calendar():
    # 2025-01-01 .. 2027-12-31 inclusive = 1096 days.
    dates = spark.range(0, 1096).select(
        F.date_add(F.to_date(F.lit("2025-01-01")), F.col("id").cast("INT")).alias(
            "full_date"
        )
    )

    return dates.select(
        F.date_format("full_date", "yyyyMMdd").cast("INT").alias("date_key"),
        F.col("full_date"),
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month_number"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week_number"),
        F.date_format("full_date", "EEEE").alias("day_of_week_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.date_format("full_date", "yyyy-MM").alias("year_month"),
    )
