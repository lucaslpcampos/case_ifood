# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 — Gold: Cumulative Tables (FULL OUTER JOIN D-1 ⟕ delta D)
# MAGIC Padrão **Cumulative Table Design** (DataExpert-io):
# MAGIC - `vendor_daily_cumulative` — running totals por vendor
# MAGIC - `hourly_passenger_cumulative` — running totals por hora (24 buckets)
# MAGIC
# MAGIC Transformações em **Spark SQL**, orquestração PySpark.

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

dbutils.widgets.text("catalog", "ifood")
catalog = dbutils.widgets.get("catalog")

gold_schema = f"{catalog}.{SCHEMA_GOLD}"
fact_fqn = f"{gold_schema}.fact_trip"
vendor_cum_fqn = f"{gold_schema}.vendor_daily_cumulative"
hourly_cum_fqn = f"{gold_schema}.hourly_passenger_cumulative"

print(f"fact_trip={fact_fqn}")
print(f"vendor_cum={vendor_cum_fqn}  hourly_cum={hourly_cum_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — tabelas

# COMMAND ----------

spark.sql(VENDOR_DAILY_CUMULATIVE_DDL.format(catalog=catalog, schema=SCHEMA_GOLD))
spark.sql(HOURLY_PASSENGER_CUMULATIVE_DDL.format(catalog=catalog, schema=SCHEMA_GOLD))

print("Tabelas cumulative prontas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Snapshot dates disponíveis na fact_trip

# COMMAND ----------

snapshot_dates = [
    row["d"]
    for row in spark.sql(f"""
        SELECT DISTINCT CAST(pickup_datetime AS DATE) AS d
        FROM {fact_fqn}
        ORDER BY d
    """).collect()
    if row["d"] is not None
]

print(f"{len(snapshot_dates)} snapshot dates: {snapshot_dates[0]} ... {snapshot_dates[-1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## vendor_daily_cumulative — loop por snapshot_date

# COMMAND ----------

for snap in snapshot_dates:
    snap_str = str(snap)

    spark.sql(f"""
        MERGE INTO {vendor_cum_fqn} AS t
        USING (
            WITH delta AS (
                SELECT
                    vendor_id,
                    COUNT(*)          AS trips_today,
                    SUM(total_amount) AS amount_today
                FROM {fact_fqn}
                WHERE CAST(pickup_datetime AS DATE) = DATE '{snap_str}'
                GROUP BY vendor_id
            ),
            yesterday AS (
                SELECT *
                FROM {vendor_cum_fqn}
                WHERE snapshot_date = DATE_SUB(DATE '{snap_str}', 1)
            )
            SELECT
                COALESCE(y.vendor_id, d.vendor_id)                        AS vendor_id,
                COALESCE(y.first_active_date, DATE '{snap_str}')          AS first_active_date,
                CASE WHEN d.vendor_id IS NOT NULL
                     THEN DATE '{snap_str}'
                     ELSE y.last_active_date END                          AS last_active_date,
                COALESCE(y.trips_cumulative, 0) + COALESCE(d.trips_today, 0)   AS trips_cumulative,
                COALESCE(y.amount_cumulative, 0) + COALESCE(d.amount_today, 0) AS amount_cumulative,
                DATE '{snap_str}'                                         AS snapshot_date
            FROM yesterday y
            FULL OUTER JOIN delta d ON y.vendor_id = d.vendor_id
        ) AS s
        ON t.vendor_id = s.vendor_id AND t.snapshot_date = s.snapshot_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"vendor_daily_cumulative: {spark.table(vendor_cum_fqn).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## hourly_passenger_cumulative — loop por snapshot_date

# COMMAND ----------

for snap in snapshot_dates:
    snap_str = str(snap)

    spark.sql(f"""
        MERGE INTO {hourly_cum_fqn} AS t
        USING (
            WITH delta AS (
                SELECT
                    HOUR(pickup_datetime)    AS pickup_hour,
                    COUNT(*)                 AS trips_today,
                    SUM(passenger_count)     AS passengers_today
                FROM {fact_fqn}
                WHERE CAST(pickup_datetime AS DATE) = DATE '{snap_str}'
                GROUP BY HOUR(pickup_datetime)
            ),
            yesterday AS (
                SELECT *
                FROM {hourly_cum_fqn}
                WHERE snapshot_date = DATE_SUB(DATE '{snap_str}', 1)
            )
            SELECT
                COALESCE(y.pickup_hour, d.pickup_hour)                             AS pickup_hour,
                DATE '{snap_str}'                                                  AS snapshot_date,
                COALESCE(y.trips_cumulative, 0) + COALESCE(d.trips_today, 0)       AS trips_cumulative,
                COALESCE(y.passengers_cumulative, 0) + COALESCE(d.passengers_today, 0) AS passengers_cumulative,
                CASE WHEN (COALESCE(y.trips_cumulative, 0) + COALESCE(d.trips_today, 0)) > 0
                     THEN DOUBLE(COALESCE(y.passengers_cumulative, 0) + COALESCE(d.passengers_today, 0))
                          / (COALESCE(y.trips_cumulative, 0) + COALESCE(d.trips_today, 0))
                     ELSE 0.0 END                                                  AS avg_passengers
            FROM yesterday y
            FULL OUTER JOIN delta d ON y.pickup_hour = d.pickup_hour
        ) AS s
        ON t.pickup_hour = s.pickup_hour AND t.snapshot_date = s.snapshot_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

print(f"hourly_passenger_cumulative: {spark.table(hourly_cum_fqn).count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check

# COMMAND ----------

display(spark.sql(f"""
    SELECT snapshot_date, SUM(trips_cumulative) AS total_trips
    FROM {vendor_cum_fqn}
    GROUP BY snapshot_date
    ORDER BY snapshot_date DESC
    LIMIT 10
"""))

display(spark.sql(f"""
    SELECT pickup_hour, avg_passengers
    FROM {hourly_cum_fqn}
    WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM {hourly_cum_fqn})
    ORDER BY pickup_hour
"""))

dbutils.notebook.exit("OK")
