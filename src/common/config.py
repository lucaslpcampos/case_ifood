"""Configuracoes operacionais compartilhadas do pipeline."""

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


def get_volume_root(catalog: str) -> str:
    """
    Monta o path raiz do volume usado pela landing zone.

    Args:
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        Path raiz do volume no formato `/Volumes/<catalog>/<schema>/<volume>`.
    """
    return f"/Volumes/{catalog}/{SCHEMA_LANDING}/{VOLUME_NAME}"


def get_checkpoint_root(catalog: str) -> str:
    """
    Monta o path raiz usado para checkpoints do pipeline.

    Args:
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        Path raiz dos checkpoints dentro do volume da landing.
    """
    return f"{get_volume_root(catalog)}/_checkpoints"


def get_landing_path(catalog: str, taxi_type: str) -> str:
    """
    Retorna o path base da landing para um tipo de taxi.

    Args:
        catalog: Nome do catalogo Unity Catalog.
        taxi_type: Tipo de taxi configurado em `TAXI_CONFIGS`.

    Returns:
        Path base da landing do tipo de taxi informado.
    """
    return f"{get_volume_root(catalog)}/{TAXI_CONFIGS[taxi_type]['landing_subpath']}"


def get_landing_month_path(catalog: str, taxi_type: str, year: int, month: int) -> str:
    """
    Retorna o path particionado da landing por ano e mes.

    Args:
        catalog: Nome do catalogo Unity Catalog.
        taxi_type: Tipo de taxi configurado em `TAXI_CONFIGS`.
        year: Ano da particao.
        month: Mes da particao.

    Returns:
        Path da landing para o ano e mes informados.
    """
    return (
        f"{get_landing_path(catalog, taxi_type)}"
        f"/year={year}/month={month:02d}"
    )


def get_fqn(catalog: str, schema: str, table: str) -> str:
    """
    Monta o nome totalmente qualificado de uma tabela.

    Args:
        catalog: Nome do catalogo Unity Catalog.
        schema: Nome do schema da tabela.
        table: Nome da tabela.

    Returns:
        FQN da tabela no formato `<catalog>.<schema>.<table>`.
    """
    return f"{catalog}.{schema}.{table}"
