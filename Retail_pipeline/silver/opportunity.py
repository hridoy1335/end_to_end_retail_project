from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_text(name: str):
    """Trim a string column and normalize empty / whitespace-only values to NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed != "", trimmed)


@dp.table(
    name="opportunity",
    comment=(
        "Silver opportunity streaming table (no SCD history, not a materialized view). "
        "Streams retail.bronze.opportunity, trimming text fields with empty strings "
        "normalized to NULL, casting Amount to DECIMAL(18,2) and Probability to "
        "DOUBLE, and normalizing booleans to false when absent. Only columns that "
        "carry data in the source are published; empty source fields (AccountId, "
        "NextStep, ContactId, Pricebook2Id) are omitted. Rows missing opportunity_id "
        "fail the update; rows missing name, stage, or close date, or with a negative "
        "amount are dropped. Probability outside 0-100 is logged as a warning. "
        "opportunity_name is the join key used by silver.transactions."
    ),
    cluster_by=["close_date"],
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "data_quality": "silver",
    },
)
@dp.expect_or_fail("opportunity_id is present", "opportunity_id IS NOT NULL")
@dp.expect_or_drop("opportunity_name is populated", "opportunity_name IS NOT NULL")
@dp.expect_or_drop("stage_name is populated", "stage_name IS NOT NULL")
@dp.expect_or_drop("close_date is populated", "close_date IS NOT NULL")
@dp.expect_or_drop(
    "amount is non-negative", "amount IS NOT NULL AND amount >= 0"
)
@dp.expect(
    "probability is within 0-100",
    "probability IS NULL OR (probability >= 0 AND probability <= 100)",
)
def opportunity():
    # Bronze opportunity is a plain append-only streaming table (no Auto CDC
    # history columns), so a plain streaming read works.
    bronze = spark.readStream.table("retail.bronze.opportunity")

    return bronze.select(
        # Identity
        clean_text("Id").alias("opportunity_id"),
        clean_text("Name").alias("opportunity_name"),
        clean_text("Description").alias("description"),
        # Pipeline
        clean_text("StageName").alias("stage_name"),
        F.col("Amount").cast("DECIMAL(18,2)").alias("amount"),
        F.col("Probability").cast("DOUBLE").alias("probability"),
        F.col("CloseDate").cast("DATE").alias("close_date"),
        clean_text("Type").alias("opportunity_type"),
        clean_text("LeadSource").alias("lead_source"),
        # Outcome
        F.coalesce(F.col("IsClosed"), F.lit(False)).alias("is_closed"),
        F.coalesce(F.col("IsWon"), F.lit(False)).alias("is_won"),
        clean_text("ForecastCategoryName").alias("forecast_category"),
        F.coalesce(F.col("HasOpportunityLineItem"), F.lit(False)).alias(
            "has_line_item"
        ),
        # Ownership and audit
        clean_text("OwnerId").alias("owner_id"),
        F.col("CreatedDate").alias("created_date"),
        F.col("LastModifiedDate").alias("last_modified_date"),
        F.col("SystemModstamp").alias("system_modstamp"),
        # Processing time: when this row was written to silver
        F.current_timestamp().alias("updated_at"),
    )
