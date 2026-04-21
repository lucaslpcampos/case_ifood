from pyspark.sql import Row

from common.quality import apply_silver_rules


def test_apply_silver_rules_routes_valid_and_invalid_rows(spark):
    df = spark.createDataFrame(
        [
            Row(
                VendorID=1,
                passenger_count=2,
                total_amount=15.5,
                pickup_datetime="2023-05-10 10:00:00",
                dropoff_datetime="2023-05-10 10:20:00",
            ),
            Row(
                VendorID=1,
                passenger_count=None,
                total_amount=12.0,
                pickup_datetime="2023-05-10 11:00:00",
                dropoff_datetime="2023-05-10 11:10:00",
            ),
        ]
    )

    clean_df, quarantine_df = apply_silver_rules(
        df,
        "2023-01-01",
        "2023-06-01",
    )

    assert clean_df.count() == 1
    assert quarantine_df.count() == 1

    rejected = quarantine_df.select("_rejection_reasons").collect()[0]["_rejection_reasons"]
    assert "passenger_count_not_null" in rejected


def test_apply_silver_rules_flags_out_of_range_rows(spark):
    df = spark.createDataFrame(
        [
            Row(
                VendorID=2,
                passenger_count=1,
                total_amount=20.0,
                pickup_datetime="2022-12-31 23:55:00",
                dropoff_datetime="2023-01-01 00:10:00",
            )
        ]
    )

    clean_df, quarantine_df = apply_silver_rules(
        df,
        "2023-01-01",
        "2023-06-01",
    )

    assert clean_df.count() == 0
    rejected = quarantine_df.select("_rejection_reasons").collect()[0]["_rejection_reasons"]
    assert "pickup_in_study_range" in rejected
