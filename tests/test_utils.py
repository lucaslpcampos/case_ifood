from pyspark.sql import Row

from common.utils import trip_surrogate_key


def test_trip_surrogate_key_is_stable_for_identical_rows(spark):
    df = spark.createDataFrame(
        [
            Row(
                VendorID=1,
                pickup_datetime="2023-05-01 08:00:00",
                dropoff_datetime="2023-05-01 08:15:00",
                passenger_count=2,
                trip_distance=3.5,
                RatecodeID=1.0,
                store_and_fwd_flag="N",
                PULocationID=100,
                DOLocationID=101,
                payment_type=1.0,
                fare_amount=10.0,
                extra=0.5,
                mta_tax=0.5,
                tip_amount=2.0,
                tolls_amount=0.0,
                improvement_surcharge=1.0,
                total_amount=14.0,
                congestion_surcharge=2.5,
                airport_fee=0.0,
            ),
            Row(
                VendorID=1,
                pickup_datetime="2023-05-01 08:00:00",
                dropoff_datetime="2023-05-01 08:15:00",
                passenger_count=2,
                trip_distance=3.5,
                RatecodeID=1.0,
                store_and_fwd_flag="N",
                PULocationID=100,
                DOLocationID=101,
                payment_type=1.0,
                fare_amount=10.0,
                extra=0.5,
                mta_tax=0.5,
                tip_amount=2.0,
                tolls_amount=0.0,
                improvement_surcharge=1.0,
                total_amount=14.0,
                congestion_surcharge=2.5,
                airport_fee=0.0,
            ),
        ]
    ).withColumn("tripsk", trip_surrogate_key("yellow"))

    keys = [row["tripsk"] for row in df.select("tripsk").collect()]
    assert keys[0] == keys[1]


def test_trip_surrogate_key_changes_when_business_fields_change(spark):
    df = spark.createDataFrame(
        [
            Row(
                VendorID=1,
                pickup_datetime="2023-05-01 08:00:00",
                dropoff_datetime="2023-05-01 08:15:00",
                passenger_count=2,
                trip_distance=3.5,
                RatecodeID=1.0,
                store_and_fwd_flag="N",
                PULocationID=100,
                DOLocationID=101,
                payment_type=1.0,
                fare_amount=10.0,
                extra=0.5,
                mta_tax=0.5,
                tip_amount=2.0,
                tolls_amount=0.0,
                improvement_surcharge=1.0,
                total_amount=14.0,
                congestion_surcharge=2.5,
                airport_fee=0.0,
            ),
            Row(
                VendorID=1,
                pickup_datetime="2023-05-01 08:00:00",
                dropoff_datetime="2023-05-01 08:15:00",
                passenger_count=3,
                trip_distance=3.5,
                RatecodeID=1.0,
                store_and_fwd_flag="N",
                PULocationID=100,
                DOLocationID=101,
                payment_type=1.0,
                fare_amount=10.0,
                extra=0.5,
                mta_tax=0.5,
                tip_amount=2.0,
                tolls_amount=0.0,
                improvement_surcharge=1.0,
                total_amount=14.0,
                congestion_surcharge=2.5,
                airport_fee=0.0,
            ),
        ]
    ).withColumn("tripsk", trip_surrogate_key("yellow"))

    keys = [row["tripsk"] for row in df.select("tripsk").collect()]
    assert keys[0] != keys[1]
