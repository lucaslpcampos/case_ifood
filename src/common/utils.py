"""Helpers utilitarios compartilhados entre notebooks e jobs do pipeline."""

import uuid
from datetime import datetime, timezone

from pyspark.sql.column import Column
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, lit, sha2


def new_batch_id() -> str:
    """
    Gera um identificador unico de batch para execucoes do pipeline.

    Args:
        None.

    Returns:
        Identificador composto por timestamp UTC e sufixo aleatorio curto.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{timestamp}_{uuid.uuid4().hex[:8]}"


def trip_surrogate_key(taxi_type: str) -> Column:
    """
    Monta a expressao Spark usada para gerar a surrogate key da viagem.

    Args:
        taxi_type: Tipo de taxi da origem, usado como parte da chave e para campos especificos.

    Returns:
        Expressao `Column` com o hash SHA-256 da combinacao de atributos da viagem.
    """
    key_parts = [
        lit(taxi_type),
        coalesce(col("VendorID").cast("string"), lit("__null__")),
        coalesce(col("pickup_datetime").cast("string"), lit("__null__")),
        coalesce(col("dropoff_datetime").cast("string"), lit("__null__")),
        coalesce(col("passenger_count").cast("string"), lit("__null__")),
        coalesce(col("trip_distance").cast("string"), lit("__null__")),
        coalesce(col("RatecodeID").cast("string"), lit("__null__")),
        coalesce(col("store_and_fwd_flag").cast("string"), lit("__null__")),
        coalesce(col("PULocationID").cast("string"), lit("__null__")),
        coalesce(col("DOLocationID").cast("string"), lit("__null__")),
        coalesce(col("payment_type").cast("string"), lit("__null__")),
        coalesce(col("fare_amount").cast("string"), lit("__null__")),
        coalesce(col("extra").cast("string"), lit("__null__")),
        coalesce(col("mta_tax").cast("string"), lit("__null__")),
        coalesce(col("tip_amount").cast("string"), lit("__null__")),
        coalesce(col("tolls_amount").cast("string"), lit("__null__")),
        coalesce(col("improvement_surcharge").cast("string"), lit("__null__")),
        coalesce(col("total_amount").cast("string"), lit("__null__")),
        coalesce(col("congestion_surcharge").cast("string"), lit("__null__")),
        coalesce(col("airport_fee").cast("string"), lit("__null__")),
    ]
    if taxi_type == "green":
        key_parts.extend(
            [
                coalesce(col("ehail_fee").cast("string"), lit("__null__")),
                coalesce(col("trip_type").cast("string"), lit("__null__")),
            ]
        )

    return sha2(concat_ws("|", *key_parts), 256)


def ensure_schemas(spark: SparkSession, catalog: str, schemas: list[str]) -> None:
    """
    Garante a existencia do catalogo e dos schemas informados.

    Args:
        spark: Sessao Spark ativa.
        catalog: Nome do catalogo Unity Catalog.
        schemas: Lista de schemas que devem existir.

    Returns:
        None. A funcao executa DDLs diretamente no catalogo.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    for schema in schemas:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def ensure_volume(
    spark: SparkSession,
    catalog: str,
    schema: str,
    volume_name: str,
) -> None:
    """
    Garante a existencia de um volume no Unity Catalog.

    Args:
        spark: Sessao Spark ativa.
        catalog: Nome do catalogo Unity Catalog.
        schema: Nome do schema que hospedara o volume.
        volume_name: Nome do volume a ser criado caso nao exista.

    Returns:
        None. A funcao executa o DDL de criacao do volume.
    """
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}")
