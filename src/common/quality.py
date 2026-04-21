from pyspark.sql.functions import (
    array,
    array_compact,
    coalesce,
    col,
    current_timestamp,
    lit,
    when,
)

def silver_rules(study_start_date, study_end_exclusive):
    return {
        "passenger_count_not_null": col("passenger_count").isNotNull(),
        "passenger_count_positive": col("passenger_count") > 0,
        "dropoff_after_pickup": col("dropoff_datetime") > col("pickup_datetime"),
        "total_amount_non_negative": col("total_amount") >= 0,
        "pickup_not_null": col("pickup_datetime").isNotNull(),
        "dropoff_not_null": col("dropoff_datetime").isNotNull(),
        "vendor_not_null": col("VendorID").isNotNull(),
        "pickup_in_study_range": (
            (col("pickup_datetime") >= lit(study_start_date).cast("timestamp"))
            & (col("pickup_datetime") < lit(study_end_exclusive).cast("timestamp"))
        ),
    }


QUALITY_RULESET_KEYS = {
    "trip_silver": silver_rules,
}


def apply_silver_rules(df, study_start_date, study_end_exclusive):
    rules = silver_rules(study_start_date, study_end_exclusive)
    passes = lit(True)
    for predicate in rules.values():
        passes = passes & coalesce(predicate, lit(False))

    clean = df.filter(passes)
    rejection_reasons = array_compact(
        array(
            *[
                when(~coalesce(predicate, lit(False)), lit(rule_name)).otherwise(lit(None))
                for rule_name, predicate in rules.items()
            ]
        )
    )
    quarantined = (
        df.filter(~passes)
        .withColumn("_quarantined_at", current_timestamp())
        .withColumn("_rejection_reasons", rejection_reasons)
    )
    return clean, quarantined


GOLD_CONSTRAINTS = {
    "fact_trip": [
        "ALTER TABLE {fqn} ADD CONSTRAINT ck_positive_amount CHECK (total_amount >= 0)",
        "ALTER TABLE {fqn} ADD CONSTRAINT ck_valid_passenger CHECK (passenger_count > 0)",
        "ALTER TABLE {fqn} ADD CONSTRAINT ck_valid_duration CHECK (duration_seconds >= 0)",
    ],
}


def apply_gold_constraints(spark, fqn, table_key):
    for ddl_template in GOLD_CONSTRAINTS.get(table_key, []):
        try:
            spark.sql(ddl_template.format(fqn=fqn))
        except Exception as exc:
            if "CONSTRAINT_ALREADY_EXISTS" in str(exc) or "already exists" in str(exc):
                continue
            raise
