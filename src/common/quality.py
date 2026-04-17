# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Quality Rules
# MAGIC Regras de DQ para a camada Silver (clean + quarantine split).
# MAGIC DQ Gold usa **Delta Constraints** (enforcement nativo na escrita).

# COMMAND ----------

from pyspark.sql.functions import (
    array,
    array_compact,
    col,
    current_timestamp,
    lit,
    when,
)

# COMMAND ----------

SILVER_RULES = {
    "passenger_count_positive": col("passenger_count") > 0,
    "dropoff_after_pickup": col("dropoff_datetime") > col("pickup_datetime"),
    "total_amount_non_negative": col("total_amount") >= 0,
    "pickup_not_null": col("pickup_datetime").isNotNull(),
    "dropoff_not_null": col("dropoff_datetime").isNotNull(),
    "vendor_not_null": col("VendorID").isNotNull(),
}

# COMMAND ----------

def apply_silver_rules(df):
    passes = lit(True)
    for pred in SILVER_RULES.values():
        passes = passes & pred
    clean = df.filter(passes)

    rejection_reasons = array_compact(
        array(*[
            when(~pred, lit(name)).otherwise(lit(None))
            for name, pred in SILVER_RULES.items()
        ])
    )
    bad = (
        df.filter(~passes)
        .withColumn("_quarantined_at", current_timestamp())
        .withColumn("_rejection_reasons", rejection_reasons)
    )
    return clean, bad

# COMMAND ----------

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
        except Exception as e:
            if "CONSTRAINT_ALREADY_EXISTS" in str(e) or "already exists" in str(e):
                pass
            else:
                raise
