# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Schemas
# MAGIC DDL strings para criação de tabelas + StructTypes de referência.
# MAGIC Todas as tabelas usam **Liquid Clustering** (`CLUSTER BY`).

# COMMAND ----------

def bronze_ddl(catalog, schema, table, pickup_col, dropoff_col):
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
        VendorID              LONG,
        {pickup_col}          TIMESTAMP,
        {dropoff_col}         TIMESTAMP,
        passenger_count       DOUBLE,
        trip_distance         DOUBLE,
        RatecodeID            DOUBLE,
        store_and_fwd_flag    STRING,
        PULocationID          LONG,
        DOLocationID          LONG,
        payment_type          LONG,
        fare_amount           DOUBLE,
        extra                 DOUBLE,
        mta_tax               DOUBLE,
        tip_amount            DOUBLE,
        tolls_amount          DOUBLE,
        improvement_surcharge DOUBLE,
        total_amount          DOUBLE,
        congestion_surcharge  DOUBLE,
        airport_fee           DOUBLE,
        _ingestion_ts         TIMESTAMP,
        _source_file          STRING,
        _batch_id             STRING,
        _rescued_data         STRING
    )
    USING DELTA
    CLUSTER BY ({pickup_col})
    """

# COMMAND ----------

def silver_ddl(catalog, schema, table):
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
        VendorID              INT,
        passenger_count       INT,
        total_amount          DECIMAL(10,2),
        pickup_datetime       TIMESTAMP,
        dropoff_datetime      TIMESTAMP,
        _trip_sk              STRING NOT NULL,
        _ingestion_ts         TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (pickup_datetime)
    """

# COMMAND ----------

def silver_quarantine_ddl(catalog, schema, table):
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
        VendorID              LONG,
        passenger_count       DOUBLE,
        total_amount          DOUBLE,
        pickup_datetime       TIMESTAMP,
        dropoff_datetime      TIMESTAMP,
        _ingestion_ts         TIMESTAMP,
        _quarantined_at       TIMESTAMP,
        _rejection_reasons    ARRAY<STRING>
    )
    USING DELTA
    """

# COMMAND ----------

DIM_DATE_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dim_date (
    date_key       INT,
    full_date      DATE,
    year           INT,
    quarter        INT,
    month          INT,
    day            INT,
    day_of_week    INT,
    week_of_year   INT,
    is_weekend     BOOLEAN
)
USING DELTA
CLUSTER BY (full_date)
"""

DIM_VENDOR_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dim_vendor (
    vendor_id        INT,
    vendor_name      STRING,
    vendor_short_name STRING
)
USING DELTA
"""

FACT_TRIP_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.fact_trip (
    _trip_sk           STRING NOT NULL,
    date_key           INT,
    vendor_id          INT,
    passenger_count    INT,
    total_amount       DECIMAL(10,2),
    pickup_datetime    TIMESTAMP,
    dropoff_datetime   TIMESTAMP,
    duration_seconds   LONG,
    taxi_type          STRING,
    _ingestion_ts      TIMESTAMP
)
USING DELTA
CLUSTER BY (pickup_datetime)
"""

VENDOR_DAILY_CUMULATIVE_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.vendor_daily_cumulative (
    vendor_id          INT,
    first_active_date  DATE,
    last_active_date   DATE,
    trips_cumulative   LONG,
    amount_cumulative  DECIMAL(18,2),
    snapshot_date      DATE
)
USING DELTA
CLUSTER BY (snapshot_date)
"""

HOURLY_PASSENGER_CUMULATIVE_DDL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.hourly_passenger_cumulative (
    pickup_hour            INT,
    snapshot_date          DATE,
    trips_cumulative       LONG,
    passengers_cumulative  LONG,
    avg_passengers         DOUBLE
)
USING DELTA
CLUSTER BY (snapshot_date)
"""
