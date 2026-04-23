"""Funcoes de ingestao da landing zone para a camada Bronze."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.column import Column
from pyspark.sql.functions import coalesce, col, current_timestamp, get_json_object, lit
from pyspark.sql.streaming import StreamingQuery

from common.config import (
    SCHEMA_BRONZE,
    TAXI_CONFIGS,
    get_checkpoint_root,
    get_fqn,
    get_landing_path,
)
from common.schemas import get_schema_columns, render_ddl
from common.utils import ensure_schemas, new_batch_id


def _rescued_key_candidates(column_name: str) -> list[str]:
    """
    Gera as chaves candidatas para recuperar um campo a partir de `_rescued_data`.

    Args:
        column_name: Nome canonico da coluna Bronze.

    Returns:
        Lista deduplicada de nomes possiveis da chave no payload resgatado.
    """
    candidates = [column_name]
    if column_name:
        candidates.append(f"{column_name[0].upper()}{column_name[1:]}")

    deduped_candidates = []
    for candidate in candidates:
        if candidate not in deduped_candidates:
            deduped_candidates.append(candidate)
    return deduped_candidates


def _bronze_value(df: DataFrame, column_name: str, data_type: str) -> Column:
    """
    Resolve o valor final de uma coluna Bronze a partir da origem e de `_rescued_data`.

    Args:
        df: DataFrame enriquecido lido da landing.
        column_name: Nome da coluna Bronze a ser projetada.
        data_type: Tipo final da coluna na Bronze.

    Returns:
        Expressao Spark com o valor consolidado e alias da coluna alvo.
    """
    primary_value = (
        col(column_name).cast(data_type)
        if column_name in df.columns
        else lit(None).cast(data_type)
    )

    if column_name in {"_ingestion_ts", "_source_file", "_batch_id", "_rescued_data"}:
        return primary_value.alias(column_name)

    rescued_value = lit(None).cast(data_type)
    for rescued_key in _rescued_key_candidates(column_name):
        rescued_value = coalesce(
            rescued_value,
            get_json_object(col("_rescued_data"), f"$.{rescued_key}").cast(data_type),
        )
    return coalesce(primary_value, rescued_value).alias(column_name)


def _project_bronze_columns(df: DataFrame, schema_key: str) -> list[Column]:
    """
    Monta a lista de colunas projetadas para o schema Bronze informado.

    Args:
        df: DataFrame enriquecido lido da landing.
        schema_key: Chave do schema Bronze registrada em `TABLE_SCHEMAS`.

    Returns:
        Lista de expressoes Spark na ordem declarada do schema Bronze.
    """
    return [
        _bronze_value(df, column_name, data_type)
        for column_name, data_type in get_schema_columns(schema_key)
    ]


def build_bronze_context(taxi_type: str, catalog: str) -> dict[str, object]:
    """
    Monta o contexto operacional usado pelo job de landing para Bronze.

    Args:
        taxi_type: Tipo de taxi configurado em `TAXI_CONFIGS`.
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        Dicionario com paths, configuracoes e FQNs usados no processamento.
    """
    taxi_cfg = TAXI_CONFIGS[taxi_type]
    return {
        "taxi_type": taxi_type,
        "catalog": catalog,
        "taxi_cfg": taxi_cfg,
        "landing_path": get_landing_path(catalog, taxi_type),
        "checkpoint_path": f"{get_checkpoint_root(catalog)}/{taxi_type}_bronze",
        "schema_location": f"{get_checkpoint_root(catalog)}/{taxi_type}_bronze_schema",
        "bronze_fqn": get_fqn(catalog, SCHEMA_BRONZE, taxi_cfg["bronze_table"]),
    }


def ensure_bronze_table(spark: SparkSession, context: dict[str, object]) -> None:
    """
    Garante a existencia da tabela Bronze de destino.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_bronze_context`.

    Returns:
        None. A funcao executa DDL de criacao da tabela Bronze.
    """
    taxi_cfg = context["taxi_cfg"]
    ensure_schemas(spark, context["catalog"], [SCHEMA_BRONZE])
    spark.sql(
        render_ddl(
            taxi_cfg["bronze_schema_key"],
            catalog=context["catalog"],
            schema=SCHEMA_BRONZE,
            table=taxi_cfg["bronze_table"],
        )
    )


def build_raw_stream_df(spark: SparkSession, context: dict[str, object]) -> DataFrame:
    """
    Cria o stream bruto da landing usando Auto Loader.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_bronze_context`.

    Returns:
        DataFrame de streaming com os arquivos da landing e schema evolution em rescue mode.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", context["schema_location"])
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .load(context["landing_path"])
    )


def enrich_stream_df(raw_stream_df: DataFrame, batch_id: str) -> DataFrame:
    """
    Adiciona metadados tecnicos ao stream bruto antes da projecao Bronze.

    Args:
        raw_stream_df: DataFrame de streaming lido da landing.
        batch_id: Identificador da execucao atual.

    Returns:
        DataFrame de streaming com colunas tecnicas de ingestao.
    """
    return (
        raw_stream_df.withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn("_batch_id", lit(batch_id))
    )


def build_bronze_stream_df(
    enriched_stream_df: DataFrame,
    context: dict[str, object],
) -> DataFrame:
    """
    Projeta o stream enriquecido para o contrato final da Bronze.

    Args:
        enriched_stream_df: DataFrame de streaming com metadados tecnicos.
        context: Contexto operacional retornado por `build_bronze_context`.

    Returns:
        DataFrame de streaming projetado no schema Bronze configurado.
    """
    taxi_cfg = context["taxi_cfg"]
    return enriched_stream_df.select(
        *_project_bronze_columns(enriched_stream_df, taxi_cfg["bronze_schema_key"])
    )


def write_bronze_stream(stream_df: DataFrame, context: dict[str, object]) -> StreamingQuery:
    """
    Persiste o stream Bronze em Delta usando trigger `availableNow`.

    Args:
        stream_df: DataFrame de streaming projetado para a Bronze.
        context: Contexto operacional retornado por `build_bronze_context`.

    Returns:
        Objeto `StreamingQuery` finalizado apos a escrita.
    """
    query = (
        stream_df.writeStream.format("delta")
        .option("checkpointLocation", context["checkpoint_path"])
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(context["bronze_fqn"])
    )
    query.awaitTermination()
    return query


def summarize_bronze(
    spark: SparkSession,
    context: dict[str, object],
    batch_id: str,
) -> dict[str, object]:
    """
    Resume o resultado da carga Bronze e destaca registros com `_rescued_data`.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_bronze_context`.
        batch_id: Identificador da execucao atual.

    Returns:
        Dicionario com metricas basicas da Bronze e informacoes do batch processado.
    """
    bronze = spark.table(context["bronze_fqn"])
    row_count = bronze.count()
    rescued_count = bronze.filter(col("_rescued_data").isNotNull()).count()

    print(f"Bronze {context['bronze_fqn']}: {row_count:,} rows  batch_id={batch_id}")
    print(f"Rescued: {rescued_count:,}")

    if rescued_count > 0:
        sample_rows = (
            bronze.filter(col("_rescued_data").isNotNull())
            .select("_source_file", "_rescued_data")
            .limit(3)
            .collect()
        )
        print("WARNING: rescued data detected in Bronze.")
        print("Sample rescued payloads (file -> payload):")
        for row in sample_rows:
            print(f"  {row['_source_file']} -> {row['_rescued_data']}")
    else:
        print("No rescued data detected in Bronze.")

    return {
        "rows": row_count,
        "batch_id": batch_id,
        "rescued_rows": rescued_count,
        "bronze_fqn": context["bronze_fqn"],
    }
