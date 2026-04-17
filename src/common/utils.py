# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Utils
# MAGIC Helpers compartilhados: surrogate key, batch_id, setup de schemas.

# COMMAND ----------

import uuid
from datetime import datetime, timezone

from pyspark.sql.functions import col, concat_ws, sha2

# COMMAND ----------

def new_batch_id():
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"


def trip_surrogate_key():
    return sha2(
        concat_ws(
            "|",
            col("VendorID").cast("string"),
            col("pickup_datetime").cast("string"),
            col("dropoff_datetime").cast("string"),
            col("total_amount").cast("string"),
        ),
        256,
    )

# COMMAND ----------

def ensure_schemas(spark, catalog, schemas):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def ensure_volume(spark, catalog, schema, volume_name):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")
