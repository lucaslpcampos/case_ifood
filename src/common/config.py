MONTHS = [(2023, month) for month in range(1, 6)]

SCHEMA_LANDING = "taxi_nyc_landing"
SCHEMA_BRONZE = "taxi_nyc_bronze"
SCHEMA_SILVER = "taxi_nyc_silver"
SCHEMA_GOLD = "taxi_nyc_gold"
VOLUME_NAME = "raw"
STUDY_START_DATE = "2023-01-01"
STUDY_END_EXCLUSIVE = "2023-06-01"

DOWNLOAD_TIMEOUT_SECONDS = 120
DOWNLOAD_MAX_RETRIES = 3


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
        "bronze_schema_key": "yellow_bronze",
        "silver_schema_key": "trip_silver",
        "quarantine_schema_key": "trip_silver_quarantine",
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
        "bronze_schema_key": "green_bronze",
        "silver_schema_key": "trip_silver",
        "quarantine_schema_key": "trip_silver_quarantine",
    },
}

FACT_SOURCE_TAXI_TYPES = ("yellow", "green")


def get_volume_root(catalog):
    return f"/Volumes/{catalog}/{SCHEMA_LANDING}/{VOLUME_NAME}"


def get_checkpoint_root(catalog):
    return f"{get_volume_root(catalog)}/_checkpoints"


def get_landing_path(catalog, taxi_type):
    return f"{get_volume_root(catalog)}/{TAXI_CONFIGS[taxi_type]['landing_subpath']}"


def get_fqn(catalog, schema, table):
    return f"{catalog}.{schema}.{table}"
