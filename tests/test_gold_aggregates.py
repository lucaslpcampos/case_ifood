from datetime import datetime, timezone
from decimal import Decimal

from pipeline.processing.gold_aggregates import (
    build_agg_hourly_daily_df,
    build_agg_monthly_taxi_df,
)


def test_build_agg_monthly_taxi_df_matches_expected_rollup(spark):
    fact_df = spark.createDataFrame(
        [
            (
                "trip-1",
                20230501,
                1,
                2,
                Decimal("10.00"),
                datetime(2023, 5, 1, 8, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 5, 1, 8, 15, 0, tzinfo=timezone.utc),
                900,
                "yellow",
                datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc),
            ),
            (
                "trip-2",
                20230502,
                1,
                1,
                Decimal("20.00"),
                datetime(2023, 5, 2, 9, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 5, 2, 9, 20, 0, tzinfo=timezone.utc),
                1200,
                "yellow",
                datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc),
            ),
        ],
        [
            "tripsk",
            "date_key",
            "vendor_id",
            "passenger_count",
            "total_amount",
            "pickup_datetime",
            "dropoff_datetime",
            "duration_seconds",
            "taxi_type",
            "_ingestion_ts",
        ],
    )
    fact_df.createOrReplaceTempView("fact_trip_unit_test")

    agg_df = build_agg_monthly_taxi_df(spark, "fact_trip_unit_test")
    row = agg_df.collect()[0]

    assert row["year"] == 2023
    assert row["month"] == 5
    assert row["taxi_type"] == "yellow"
    assert row["trip_count"] == 2
    assert float(row["total_amount_sum"]) == 30.0
    assert row["passenger_count_sum"] == 3
    assert row["avg_total_amount"] == 15.0
    assert row["avg_passenger_count"] == 1.5


def test_build_agg_hourly_daily_df_preserves_daily_hourly_grain(spark):
    fact_df = spark.createDataFrame(
        [
            (
                "trip-1",
                20230501,
                1,
                2,
                Decimal("10.00"),
                datetime(2023, 5, 1, 8, 0, 0, tzinfo=timezone.utc),
                datetime(2023, 5, 1, 8, 15, 0, tzinfo=timezone.utc),
                900,
                "yellow",
                datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc),
            ),
            (
                "trip-2",
                20230501,
                1,
                1,
                Decimal("20.00"),
                datetime(2023, 5, 1, 8, 30, 0, tzinfo=timezone.utc),
                datetime(2023, 5, 1, 8, 45, 0, tzinfo=timezone.utc),
                900,
                "yellow",
                datetime(2026, 4, 21, 10, 0, 0, tzinfo=timezone.utc),
            ),
        ],
        [
            "tripsk",
            "date_key",
            "vendor_id",
            "passenger_count",
            "total_amount",
            "pickup_datetime",
            "dropoff_datetime",
            "duration_seconds",
            "taxi_type",
            "_ingestion_ts",
        ],
    )
    fact_df.createOrReplaceTempView("fact_trip_unit_test_hourly")

    agg_df = build_agg_hourly_daily_df(spark, "fact_trip_unit_test_hourly")
    row = agg_df.collect()[0]

    assert row["date_key"] == 20230501
    assert row["pickup_hour"] == 8
    assert row["taxi_type"] == "yellow"
    assert row["trip_count"] == 2
    assert float(row["total_amount_sum"]) == 30.0
    assert row["passenger_count_sum"] == 3
    assert row["avg_total_amount"] == 15.0
    assert row["avg_passenger_count"] == 1.5
