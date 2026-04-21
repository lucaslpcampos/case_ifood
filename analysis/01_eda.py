# Databricks notebook source

import os
import sys

if "__file__" in globals():
    HERE = os.path.dirname(os.path.abspath(__file__))
else:
    HERE = os.getcwd()

SRC_ROOT = os.path.abspath(os.path.join(HERE, "..", "src"))
if SRC_ROOT not in sys.path:
    sys.path.append(SRC_ROOT)

from common.config import (
    SCHEMA_BRONZE,
    SCHEMA_GOLD,
    SCHEMA_SILVER,
    STUDY_END_EXCLUSIVE,
    STUDY_START_DATE,
)

dbutils.widgets.text("catalog", "ifood")
catalog = dbutils.widgets.get("catalog")

yellow_silver = f"{catalog}.{SCHEMA_SILVER}.yellow_trips"
green_silver = f"{catalog}.{SCHEMA_SILVER}.green_trips"
yellow_bronze = f"{catalog}.{SCHEMA_BRONZE}.yellow_trips"
green_bronze = f"{catalog}.{SCHEMA_BRONZE}.green_trips"
fact_trip = f"{catalog}.{SCHEMA_GOLD}.fact_trip"

display(
    spark.sql(
        f"""
        SELECT
            taxi_type,
            date_format(pickup_datetime, 'yyyy-MM') AS year_month,
            COUNT(*) AS trips
        FROM {fact_trip}
        GROUP BY taxi_type, date_format(pickup_datetime, 'yyyy-MM')
        ORDER BY taxi_type, year_month
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT passenger_count, COUNT(*) AS n
        FROM {fact_trip}
        GROUP BY passenger_count
        ORDER BY passenger_count
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT
            taxi_type,
            MIN(total_amount) AS min_amt,
            MAX(total_amount) AS max_amt,
            ROUND(AVG(total_amount), 2) AS avg_amt,
            ROUND(PERCENTILE(total_amount, 0.5), 2) AS median_amt,
            ROUND(PERCENTILE(total_amount, 0.99), 2) AS p99_amt
        FROM {fact_trip}
        GROUP BY taxi_type
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT _rejection_reasons, COUNT(*) AS n
        FROM {catalog}.{SCHEMA_SILVER}.yellow_trips_quarantine
        GROUP BY _rejection_reasons
        ORDER BY n DESC
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT _rejection_reasons, COUNT(*) AS n
        FROM {catalog}.{SCHEMA_SILVER}.green_trips_quarantine
        GROUP BY _rejection_reasons
        ORDER BY n DESC
        """
    )
)

display(
    spark.sql(
        f"""
        SELECT HOUR(pickup_datetime) AS pickup_hour, COUNT(*) AS trips
        FROM {fact_trip}
        GROUP BY HOUR(pickup_datetime)
        ORDER BY pickup_hour
        """
    )
)

# Temporal validation in Bronze: records outside the study range are preserved
# in the raw layer, but should be isolated from the analytical Gold layer.
display(
    spark.sql(
        f"""
        SELECT
            taxi_type,
            COUNT(*) AS rows_outside_study_range,
            MIN(pickup_datetime) AS min_pickup_datetime,
            MAX(pickup_datetime) AS max_pickup_datetime
        FROM (
            SELECT 'yellow' AS taxi_type, tpep_pickup_datetime AS pickup_datetime
            FROM {yellow_bronze}
            WHERE tpep_pickup_datetime < TIMESTAMP '{STUDY_START_DATE}'
               OR tpep_pickup_datetime >= TIMESTAMP '{STUDY_END_EXCLUSIVE}'

            UNION ALL

            SELECT 'green' AS taxi_type, lpep_pickup_datetime AS pickup_datetime
            FROM {green_bronze}
            WHERE lpep_pickup_datetime < TIMESTAMP '{STUDY_START_DATE}'
               OR lpep_pickup_datetime >= TIMESTAMP '{STUDY_END_EXCLUSIVE}'
        )
        GROUP BY taxi_type
        ORDER BY taxi_type
        """
    )
)

# Temporal domain inspection: useful to document anomalous source dates that
# do not belong to the business scope of the case.
display(
    spark.sql(
        f"""
        SELECT
            taxi_type,
            MIN(pickup_datetime) AS min_pickup_datetime,
            MAX(pickup_datetime) AS max_pickup_datetime
        FROM (
            SELECT 'yellow' AS taxi_type, tpep_pickup_datetime AS pickup_datetime
            FROM {yellow_bronze}
            UNION ALL
            SELECT 'green' AS taxi_type, lpep_pickup_datetime AS pickup_datetime
            FROM {green_bronze}
        )
        GROUP BY taxi_type
        ORDER BY taxi_type
        """
    )
)

# Bronze consistency validation: rescued rows should not leave critical source
# attributes null when the same value is present in `_rescued_data`.
display(
    spark.sql(
        f"""
        SELECT
            taxi_type,
            COUNT(*) AS rescued_rows,
            SUM(CASE WHEN VendorID IS NULL THEN 1 ELSE 0 END) AS vendorid_nulls,
            SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END) AS passenger_count_nulls,
            SUM(CASE WHEN RatecodeID IS NULL THEN 1 ELSE 0 END) AS ratecodeid_nulls,
            SUM(CASE WHEN PULocationID IS NULL THEN 1 ELSE 0 END) AS pulocationid_nulls,
            SUM(CASE WHEN DOLocationID IS NULL THEN 1 ELSE 0 END) AS dolocationid_nulls,
            SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) AS payment_type_nulls
        FROM (
            SELECT
                'yellow' AS taxi_type,
                VendorID,
                passenger_count,
                RatecodeID,
                PULocationID,
                DOLocationID,
                payment_type
            FROM {yellow_bronze}
            WHERE _rescued_data IS NOT NULL

            UNION ALL

            SELECT
                'green' AS taxi_type,
                VendorID,
                passenger_count,
                RatecodeID,
                PULocationID,
                DOLocationID,
                payment_type
            FROM {green_bronze}
            WHERE _rescued_data IS NOT NULL
        )
        GROUP BY taxi_type
        ORDER BY taxi_type
        """
    )
)

# Bronze detailed inspection for records that still keep critical columns null
# after rescue recovery.
display(
    spark.sql(
        f"""
        SELECT
            'yellow' AS taxi_type,
            VendorID,
            passenger_count,
            RatecodeID,
            PULocationID,
            DOLocationID,
            payment_type,
            _rescued_data
        FROM {yellow_bronze}
        WHERE _rescued_data IS NOT NULL
          AND (
              VendorID IS NULL
              OR passenger_count IS NULL
              OR RatecodeID IS NULL
              OR PULocationID IS NULL
              OR DOLocationID IS NULL
              OR payment_type IS NULL
          )

        UNION ALL

        SELECT
            'green' AS taxi_type,
            VendorID,
            passenger_count,
            RatecodeID,
            PULocationID,
            DOLocationID,
            payment_type,
            _rescued_data
        FROM {green_bronze}
        WHERE _rescued_data IS NOT NULL
          AND (
              VendorID IS NULL
              OR passenger_count IS NULL
              OR RatecodeID IS NULL
              OR PULocationID IS NULL
              OR DOLocationID IS NULL
              OR payment_type IS NULL
          )
        LIMIT 50
        """
    )
)

# Null passenger_count values exist in the source Bronze layer and deserve
# explicit visibility, even though they are not caused by rescue_data.
display(
    spark.sql(
        f"""
        SELECT
            taxi_type,
            COUNT(*) AS rows_with_null_passenger_count
        FROM (
            SELECT 'yellow' AS taxi_type, passenger_count
            FROM {yellow_bronze}
            WHERE passenger_count IS NULL

            UNION ALL

            SELECT 'green' AS taxi_type, passenger_count
            FROM {green_bronze}
            WHERE passenger_count IS NULL
        )
        GROUP BY taxi_type
        ORDER BY taxi_type
        """
    )
)
