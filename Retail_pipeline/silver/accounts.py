from pyspark import pipelines as dp
from pyspark.sql import functions as F


def clean_text(name: str):
    """Trim a string column and normalize empty / whitespace-only values to NULL."""
    trimmed = F.trim(F.col(name))
    return F.when(trimmed != "", trimmed)


@dp.table(
    name="accounts",
    comment=(
        "Silver accounts streaming table holding the latest version of each account "
        "(no SCD history). Streams the current state of retail.bronze.accounts (Auto CDC "
        "SCD Type 2) with skipChangeCommits and atomically replaces each account's "
        "previous row on every new version via a REPLACE USING flow keyed on account_id "
        "and sequenced by system_modstamp. Deleted accounts are excluded, text fields "
        "are trimmed with empty strings normalized to NULL, phone numbers are reduced to "
        "digits, and websites are normalized to an https:// URL. Only columns that "
        "carry data in the source are published; empty source fields (shipping address, "
        "geolocation, postal code, parent/source/SIC, fax, revenue, employees) are omitted."
    ),
    cluster_by=["account_id"],
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days",
        "data_quality": "silver",
    },
    replace_using=["account_id"],
    sequence_by="system_modstamp",
)
@dp.expect_or_fail("account_id is present", "account_id IS NOT NULL")
@dp.expect_or_drop("account_name is populated", "account_name IS NOT NULL")
@dp.expect("phone contains 7-15 digits", "phone IS NULL OR phone RLIKE '^[0-9]{7,15}$'")
@dp.expect(
    "website is a well-formed URL",
    "website IS NULL OR website RLIKE '(?i)^https?://[a-z0-9\\-]+(\\.[a-z0-9\\-]+)+'",
)
@dp.expect("billing_country is populated", "billing_country IS NOT NULL")
def accounts():
    # skipChangeCommits: bronze is an Auto CDC (SCD Type 2) target whose merges
    # rewrite existing rows; ignore those commits and process only newly added rows.
    bronze = spark.readStream.option("skipChangeCommits", "true").table(
        "retail.bronze.accounts"
    )

    # Current state of each account, excluding soft-deleted records. Historical SCD
    # versions (__END_AT set) are filtered out, and the REPLACE USING flow keeps the
    # latest row per account_id (sequenced by system_modstamp).
    current = bronze.where(
        (F.col("__END_AT").isNull())
        & (~F.coalesce(F.col("IsDeleted"), F.lit(False)))
    )

    # Normalize contact fields.
    phone = F.regexp_replace(clean_text("Phone"), "[^0-9]", "")
    website = clean_text("Website")

    return current.select(
        # Identity
        clean_text("Id").alias("account_id"),
        clean_text("Name").alias("account_name"),
        clean_text("Type").alias("account_type"),
        # Classification
        clean_text("Industry").alias("industry"),
        clean_text("Description").alias("description"),
        # Contact
        phone.alias("phone"),
        F.when(website.rlike("(?i)^https?://"), website)
        .otherwise(F.concat(F.lit("https://"), website))
        .alias("website"),
        # Flags
        F.coalesce(F.col("IsCustomerPortal"), F.lit(False)).alias("is_customer_portal"),
        F.coalesce(F.col("IsBuyer"), F.lit(False)).alias("is_buyer"),
        # Billing address (city, state, country are the populated fields)
        clean_text("BillingCity").alias("billing_city"),
        clean_text("BillingState").alias("billing_state"),
        clean_text("BillingCountry").alias("billing_country"),
        # Ownership and audit
        clean_text("OwnerId").alias("owner_id"),
        F.col("CreatedDate").alias("created_date"),
        clean_text("CreatedById").alias("created_by_id"),
        F.col("LastModifiedDate").alias("last_modified_date"),
        clean_text("LastModifiedById").alias("last_modified_by_id"),
        F.col("SystemModstamp").alias("system_modstamp"),
        # Processing time: when this row was last written to the silver table
        F.current_timestamp().alias("updated_at"),
    )