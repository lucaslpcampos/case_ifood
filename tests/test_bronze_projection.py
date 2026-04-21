from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from common.schemas import get_schema_columns
from pipeline.ingestion.landing_to_bronze import build_bronze_stream_df


def test_build_bronze_stream_df_recovers_values_from_rescued_data(spark):
    df = spark.createDataFrame(
        [
            (
                None,
                "2023-05-01 08:00:00",
                "2023-05-01 08:15:00",
                None,
                3.5,
                None,
                "N",
                None,
                None,
                1.0,
                10.0,
                0.5,
                0.5,
                2.0,
                0.0,
                1.0,
                14.0,
                2.5,
                None,
                "2026-04-21 10:00:00",
                "/tmp/yellow_tripdata_2023-05.parquet",
                "batch-1",
                '{"VendorID":2,"passenger_count":1,"RatecodeID":1,"PULocationID":100,"DOLocationID":101,"Airport_fee":0.0}',
            )
        ],
        schema=StructType(
            [
                StructField("VendorID", DoubleType(), True),
                StructField("tpep_pickup_datetime", StringType(), True),
                StructField("tpep_dropoff_datetime", StringType(), True),
                StructField("passenger_count", DoubleType(), True),
                StructField("trip_distance", DoubleType(), True),
                StructField("RatecodeID", DoubleType(), True),
                StructField("store_and_fwd_flag", StringType(), True),
                StructField("PULocationID", DoubleType(), True),
                StructField("DOLocationID", DoubleType(), True),
                StructField("payment_type", DoubleType(), True),
                StructField("fare_amount", DoubleType(), True),
                StructField("extra", DoubleType(), True),
                StructField("mta_tax", DoubleType(), True),
                StructField("tip_amount", DoubleType(), True),
                StructField("tolls_amount", DoubleType(), True),
                StructField("improvement_surcharge", DoubleType(), True),
                StructField("total_amount", DoubleType(), True),
                StructField("congestion_surcharge", DoubleType(), True),
                StructField("airport_fee", DoubleType(), True),
                StructField("_ingestion_ts", StringType(), True),
                StructField("_source_file", StringType(), True),
                StructField("_batch_id", StringType(), True),
                StructField("_rescued_data", StringType(), True),
            ]
        ),
    )
    context = {
        "taxi_cfg": {
            "bronze_schema_key": "yellow_bronze",
        },
    }

    projected_df = build_bronze_stream_df(df, context)
    row = projected_df.collect()[0]

    assert row["VendorID"] == 2
    assert row["passenger_count"] == 1.0
    assert row["RatecodeID"] == 1.0
    assert row["PULocationID"] == 100
    assert row["DOLocationID"] == 101
    assert row["airport_fee"] == 0.0


def test_bronze_columns_keeps_green_specific_fields_out_of_yellow_schema():
    yellow_names = [name for name, _ in get_schema_columns("yellow_bronze")]
    green_names = [name for name, _ in get_schema_columns("green_bronze")]

    assert "ehail_fee" not in yellow_names
    assert "trip_type" not in yellow_names
    assert "ehail_fee" in green_names
    assert "trip_type" in green_names
