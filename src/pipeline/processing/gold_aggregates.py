from pyspark.sql import DataFrame, SparkSession

from common.config import SCHEMA_GOLD
from common.schemas import apply_table_comments, render_ddl
from common.sql import (
    build_agg_trip_hourly_daily_query,
    build_agg_trip_monthly_taxi_query,
)
from common.utils import ensure_schemas


def build_gold_aggregate_context(catalog: str) -> dict[str, str]:
    """
    Monta o contexto operacional usado pela carga dos agregados Gold.

    Args:
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        Dicionario com FQNs da fato e dos agregados de destino.
    """
    gold_schema = f"{catalog}.{SCHEMA_GOLD}"
    return {
        "catalog": catalog,
        "gold_schema": gold_schema,
        "fact_fqn": f"{gold_schema}.fact_trip",
        "agg_hourly_daily_fqn": f"{gold_schema}.agg_trip_hourly_daily",
        "agg_monthly_taxi_fqn": f"{gold_schema}.agg_trip_monthly_taxi",
    }


def ensure_gold_aggregate_tables(spark: SparkSession, context: dict[str, str]) -> None:
    """
    Garante a existencia das tabelas agregadas da camada Gold.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_gold_aggregate_context`.

    Returns:
        None. A funcao executa DDLs para preparar as tabelas agregadas.
    """
    ensure_schemas(spark, context["catalog"], [SCHEMA_GOLD])
    spark.sql(
        render_ddl("agg_trip_hourly_daily", catalog=context["catalog"], schema=SCHEMA_GOLD)
    )
    apply_table_comments(
        spark,
        "agg_trip_hourly_daily",
        catalog=context["catalog"],
        schema=SCHEMA_GOLD,
    )
    spark.sql(
        render_ddl("agg_trip_monthly_taxi", catalog=context["catalog"], schema=SCHEMA_GOLD)
    )
    apply_table_comments(
        spark,
        "agg_trip_monthly_taxi",
        catalog=context["catalog"],
        schema=SCHEMA_GOLD,
    )


def build_agg_hourly_daily_df(spark: SparkSession, fact_fqn: str) -> DataFrame:
    """
    Construi o DataFrame do agregado diario por hora e tipo de taxi.

    Args:
        spark: Sessao Spark ativa.
        fact_fqn: Nome totalmente qualificado da fato transacional.

    Returns:
        DataFrame com o agregado horario diario.
    """
    return spark.sql(build_agg_trip_hourly_daily_query(fact_fqn))


def write_overwrite_table(df: DataFrame, source_view_name: str, target_fqn: str) -> None:
    """
    Sobrescreve uma tabela Delta usando `INSERT OVERWRITE` a partir de uma view temporaria.

    Args:
        df: DataFrame que sera materializado temporariamente.
        source_view_name: Nome da view temporaria usada no overwrite.
        target_fqn: Nome totalmente qualificado da tabela de destino.

    Returns:
        None. A funcao materializa a view e executa o overwrite da tabela.
    """
    spark = df.sparkSession
    df.createOrReplaceTempView(source_view_name)
    spark.sql(
        f"""
        INSERT OVERWRITE TABLE {target_fqn}
        SELECT *
        FROM {source_view_name}
        """
    )


def write_agg_hourly_daily(agg_hourly_daily_df: DataFrame, agg_hourly_daily_fqn: str) -> None:
    """
    Persiste o agregado horario diario na tabela Gold correspondente.

    Args:
        agg_hourly_daily_df: DataFrame com o agregado horario diario.
        agg_hourly_daily_fqn: Nome totalmente qualificado da tabela de destino.

    Returns:
        None. A funcao executa o overwrite da tabela agregada.
    """
    write_overwrite_table(
        agg_hourly_daily_df,
        "agg_trip_hourly_daily_source",
        agg_hourly_daily_fqn,
    )


def build_agg_monthly_taxi_df(spark: SparkSession, fact_fqn: str) -> DataFrame:
    """
    Construi o DataFrame do agregado mensal por tipo de taxi.

    Args:
        spark: Sessao Spark ativa.
        fact_fqn: Nome totalmente qualificado da fato transacional.

    Returns:
        DataFrame com o agregado mensal por tipo de taxi.
    """
    return spark.sql(build_agg_trip_monthly_taxi_query(fact_fqn))


def write_agg_monthly_taxi(agg_monthly_taxi_df: DataFrame, agg_monthly_taxi_fqn: str) -> None:
    """
    Persiste o agregado mensal por tipo de taxi na tabela Gold correspondente.

    Args:
        agg_monthly_taxi_df: DataFrame com o agregado mensal por tipo de taxi.
        agg_monthly_taxi_fqn: Nome totalmente qualificado da tabela de destino.

    Returns:
        None. A funcao executa o overwrite da tabela agregada.
    """
    write_overwrite_table(
        agg_monthly_taxi_df,
        "agg_trip_monthly_taxi_source",
        agg_monthly_taxi_fqn,
    )


def summarize_aggregates(spark: SparkSession, context: dict[str, str]) -> dict[str, int]:
    """
    Resume o volume final gravado nas tabelas agregadas Gold.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_gold_aggregate_context`.

    Returns:
        Dicionario com a quantidade de linhas de cada agregado publicado.
    """
    hourly_daily_rows = spark.table(context["agg_hourly_daily_fqn"]).count()
    monthly_taxi_rows = spark.table(context["agg_monthly_taxi_fqn"]).count()
    print(f"agg_trip_hourly_daily: {hourly_daily_rows:,} rows")
    print(f"agg_trip_monthly_taxi: {monthly_taxi_rows:,} rows")
    return {
        "hourly_daily_rows": hourly_daily_rows,
        "monthly_taxi_rows": monthly_taxi_rows,
    }
