# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Config
# MAGIC Configurações compartilhadas do pipeline NYC Taxi.
# MAGIC Parametrização pesada (catalog, taxi_type) vem via **Job parameters → widgets**.
# MAGIC Aqui ficam apenas mapeamentos data-driven por taxi_type.

# COMMAND ----------

MONTHS = [(2023, m) for m in range(1, 6)]

TAXI_CONFIGS = {
    "yellow": {
        "source_url_template": (
            "https://d37ci6vzurychx.cloudfront.net/trip-data/"
            "yellow_tripdata_{year}-{month:02d}.parquet"
        ),
        "pickup_col": "tpep_pickup_datetime",
        "dropoff_col": "tpep_dropoff_datetime",
        "landing_subpath": "yellow",
        "bronze_table": "yellow_trips",
        "silver_table": "yellow_trips",
        "quarantine_table": "yellow_trips_quarantine",
    },
    "green": {
        "source_url_template": (
            "https://d37ci6vzurychx.cloudfront.net/trip-data/"
            "green_tripdata_{year}-{month:02d}.parquet"
        ),
        "pickup_col": "lpep_pickup_datetime",
        "dropoff_col": "lpep_dropoff_datetime",
        "landing_subpath": "green",
        "bronze_table": "green_trips",
        "silver_table": "green_trips",
        "quarantine_table": "green_trips_quarantine",
    },
}

SCHEMA_LANDING = "taxi_nyc_landing"
SCHEMA_BRONZE = "taxi_nyc_bronze"
SCHEMA_SILVER = "taxi_nyc_silver"
SCHEMA_GOLD = "taxi_nyc_gold"
VOLUME_NAME = "raw"

DOWNLOAD_TIMEOUT_SECONDS = 120
DOWNLOAD_MAX_RETRIES = 3

# COMMAND ----------

def get_volume_root(catalog):
    return f"/Volumes/{catalog}/{SCHEMA_LANDING}/{VOLUME_NAME}"


def get_checkpoint_root(catalog):
    return f"/Volumes/{catalog}/{SCHEMA_LANDING}/{VOLUME_NAME}/_checkpoints"


def get_landing_path(catalog, taxi_type):
    return f"{get_volume_root(catalog)}/{TAXI_CONFIGS[taxi_type]['landing_subpath']}"


def get_fqn(catalog, schema, table):
    return f"{catalog}.{schema}.{table}"
