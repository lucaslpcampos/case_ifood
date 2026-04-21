from delta.tables import DeltaTable
from pyspark.sql.functions import col

from common.config import (
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    STUDY_END_EXCLUSIVE,
    STUDY_START_DATE,
    TAXI_CONFIGS,
    get_fqn,
)
from common.quality import apply_silver_rules
from common.schemas import render_ddl
from common.utils import ensure_schemas, trip_surrogate_key


def build_silver_context(taxi_type, catalog):
    taxi_cfg = TAXI_CONFIGS[taxi_type]
    return {
        "taxi_type": taxi_type,
        "catalog": catalog,
        "taxi_cfg": taxi_cfg,
        "bronze_fqn": get_fqn(catalog, SCHEMA_BRONZE, taxi_cfg["bronze_table"]),
        "silver_fqn": get_fqn(catalog, SCHEMA_SILVER, taxi_cfg["silver_table"]),
        "quarantine_fqn": get_fqn(catalog, SCHEMA_SILVER, taxi_cfg["quarantine_table"]),
    }


def ensure_silver_tables(spark, context):
    taxi_cfg = context["taxi_cfg"]
    ensure_schemas(spark, context["catalog"], [SCHEMA_SILVER])
    spark.sql(
        render_ddl(
            taxi_cfg["silver_schema_key"],
            catalog=context["catalog"],
            schema=SCHEMA_SILVER,
            table=taxi_cfg["silver_table"],
        )
    )
    spark.sql(
        render_ddl(
            taxi_cfg["quarantine_schema_key"],
            catalog=context["catalog"],
            schema=SCHEMA_SILVER,
            table=taxi_cfg["quarantine_table"],
        )
    )


def build_silver_source_df(spark, context):
    taxi_cfg = context["taxi_cfg"]
    pickup_col = taxi_cfg["pickup_col"]
    dropoff_col = taxi_cfg["dropoff_col"]
    taxi_type = context["taxi_type"]

    return (
        spark.table(context["bronze_fqn"])
        .withColumn("pickup_datetime", col(pickup_col).cast("timestamp"))
        .withColumn("dropoff_datetime", col(dropoff_col).cast("timestamp"))
        .withColumn("VendorID", col("VendorID").cast("int"))
        .withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("total_amount", col("total_amount").cast("decimal(10,2)"))
        .withColumn("tripsk", trip_surrogate_key(taxi_type))
    )


def summarize_silver_source(source_df):
    rows_in = source_df.count()
    print(f"Bronze read: {rows_in:,} rows")
    return rows_in


def split_silver_clean_and_quarantine(source_df):
    return apply_silver_rules(
        source_df,
        STUDY_START_DATE,
        STUDY_END_EXCLUSIVE,
    )


def summarize_silver_quality(rows_in, clean_df, quarantine_df):
    rows_clean = clean_df.count()
    rows_quarantine = quarantine_df.count()
    quarantine_pct = rows_quarantine / max(rows_in, 1) * 100
    print(
        f"Clean: {rows_clean:,}  Quarantine: {rows_quarantine:,}  "
        f"({quarantine_pct:.1f}%)"
    )
    return {
        "rows_clean": rows_clean,
        "rows_quarantine": rows_quarantine,
        "quarantine_pct": quarantine_pct,
    }


def project_silver_df(clean_df):
    return (
        clean_df.select(
            col("VendorID"),
            col("passenger_count"),
            col("total_amount"),
            col("pickup_datetime"),
            col("dropoff_datetime"),
            col("tripsk"),
            col("_ingestion_ts"),
        )
    )


def find_duplicate_merge_keys(source_df, merge_key="tripsk"):
    return (
        source_df.groupBy(merge_key)
        .count()
        .filter(col("count") > 1)
    )


def validate_merge_source_uniqueness(source_df, merge_key="tripsk", sample_size=10):
    duplicate_keys_df = find_duplicate_merge_keys(source_df, merge_key)
    duplicate_key_count = duplicate_keys_df.count()

    if duplicate_key_count == 0:
        return {
            "duplicate_key_count": 0,
            "sample": [],
        }

    sample_rows = (
        duplicate_keys_df.orderBy(col("count").desc(), col(merge_key).asc())
        .limit(sample_size)
        .collect()
    )
    sample = [
        {
            merge_key: row[merge_key],
            "count": row["count"],
        }
        for row in sample_rows
    ]
    sample_text = ", ".join(
        f"{row[merge_key]} ({row['count']} rows)"
        for row in sample
    )

    raise ValueError(
        f"Merge source contains {duplicate_key_count} duplicated {merge_key} values. "
        "This usually indicates replay in Bronze or an insufficient merge key design. "
        f"Sample duplicated keys: {sample_text}"
    )


def merge_silver_df(spark, silver_fqn, silver_df):
    target_table = DeltaTable.forName(spark, silver_fqn)
    (
        target_table.alias("target")
        .merge(silver_df.alias("source"), "target.tripsk = source.tripsk")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def append_quarantine_df(quarantine_fqn, quarantine_df):
    rows_quarantine = quarantine_df.count()
    if rows_quarantine > 0:
        (
            quarantine_df.select(
                col("VendorID").cast("int"),
                col("passenger_count").cast("double"),
                col("total_amount").cast("double"),
                col("pickup_datetime"),
                col("dropoff_datetime"),
                col("_ingestion_ts"),
                col("_quarantined_at"),
                col("_rejection_reasons"),
            )
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quarantine_fqn)
        )
    return rows_quarantine


def validate_silver(spark, silver_fqn):
    rows_silver = spark.table(silver_fqn).count()
    duplicated_keys = spark.sql(
        f"""
        SELECT tripsk, COUNT(*) AS n
        FROM {silver_fqn}
        GROUP BY tripsk
        HAVING n > 1
        """
    ).count()
    assert duplicated_keys == 0, (
        f"FAIL: {duplicated_keys} duplicated tripsk values in {silver_fqn}"
    )
    return {
        "rows": rows_silver,
        "duplicated_keys": duplicated_keys,
    }
