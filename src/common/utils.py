import uuid
from datetime import datetime, timezone

from pyspark.sql.functions import coalesce, col, concat_ws, lit, sha2


def new_batch_id():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def trip_surrogate_key(taxi_type):
    return sha2(
        concat_ws(
            "|",
            lit(taxi_type),
            coalesce(col("VendorID").cast("string"), lit("__null__")),
            coalesce(col("pickup_datetime").cast("string"), lit("__null__")),
            coalesce(col("dropoff_datetime").cast("string"), lit("__null__")),
            coalesce(col("passenger_count").cast("string"), lit("__null__")),
            coalesce(col("trip_distance").cast("string"), lit("__null__")),
            coalesce(col("RatecodeID").cast("string"), lit("__null__")),
            coalesce(col("store_and_fwd_flag").cast("string"), lit("__null__")),
            coalesce(col("PULocationID").cast("string"), lit("__null__")),
            coalesce(col("DOLocationID").cast("string"), lit("__null__")),
            coalesce(col("payment_type").cast("string"), lit("__null__")),
            coalesce(col("fare_amount").cast("string"), lit("__null__")),
            coalesce(col("extra").cast("string"), lit("__null__")),
            coalesce(col("mta_tax").cast("string"), lit("__null__")),
            coalesce(col("tip_amount").cast("string"), lit("__null__")),
            coalesce(col("tolls_amount").cast("string"), lit("__null__")),
            coalesce(col("improvement_surcharge").cast("string"), lit("__null__")),
            coalesce(col("total_amount").cast("string"), lit("__null__")),
            coalesce(col("congestion_surcharge").cast("string"), lit("__null__")),
            coalesce(col("airport_fee").cast("string"), lit("__null__")),
        ),
        256,
    )


def ensure_schemas(spark, catalog, schemas):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def ensure_volume(spark, catalog, schema, volume_name):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")
