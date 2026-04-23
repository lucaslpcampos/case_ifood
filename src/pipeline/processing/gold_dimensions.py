from pyspark.sql import DataFrame, SparkSession

from common.config import SCHEMA_GOLD, STUDY_END_EXCLUSIVE, STUDY_START_DATE
from common.schemas import apply_table_comments, render_ddl
from common.sql import build_dim_date_query, build_dim_vendor_query
from common.utils import ensure_schemas


def build_gold_dimension_context(catalog: str) -> dict[str, str]:
    """
    Monta o contexto operacional usado pela carga das dimensoes Gold.

    Args:
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        Dicionario com FQNs e paths logicos usados pelo job de dimensoes.
    """
    gold_schema = f"{catalog}.{SCHEMA_GOLD}"
    return {
        "catalog": catalog,
        "gold_schema": gold_schema,
        "dim_date_fqn": f"{gold_schema}.dim_date",
        "dim_vendor_fqn": f"{gold_schema}.dim_vendor",
    }


def ensure_gold_dimension_tables(spark: SparkSession, context: dict[str, str]) -> None:
    """
    Garante a existencia das tabelas de dimensao na camada Gold.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_gold_dimension_context`.

    Returns:
        None. A funcao executa DDLs para preparar as dimensoes.
    """
    ensure_schemas(spark, context["catalog"], [SCHEMA_GOLD])
    spark.sql(render_ddl("dim_date", catalog=context["catalog"], schema=SCHEMA_GOLD))
    apply_table_comments(spark, "dim_date", catalog=context["catalog"], schema=SCHEMA_GOLD)
    spark.sql(render_ddl("dim_vendor", catalog=context["catalog"], schema=SCHEMA_GOLD))
    apply_table_comments(spark, "dim_vendor", catalog=context["catalog"], schema=SCHEMA_GOLD)


def build_dim_date_df(spark: SparkSession) -> DataFrame:
    """
    Construi o DataFrame da dimensao de datas.

    Args:
        spark: Sessao Spark ativa.

    Returns:
        DataFrame com uma linha por dia dentro do intervalo de estudo.
    """
    return spark.sql(build_dim_date_query(STUDY_START_DATE, STUDY_END_EXCLUSIVE))


def build_dim_vendor_df(spark: SparkSession) -> DataFrame:
    """
    Construi o DataFrame da dimensao de vendors.

    Args:
        spark: Sessao Spark ativa.

    Returns:
        DataFrame com o mapeamento de vendors suportados pela Gold.
    """
    return spark.sql(build_dim_vendor_query())


def write_delta_table(df: DataFrame, target_fqn: str) -> None:
    """
    Sobrescreve integralmente uma tabela Delta com o DataFrame informado.

    Args:
        df: DataFrame a ser persistido.
        target_fqn: Nome totalmente qualificado da tabela de destino.

    Returns:
        None. A funcao grava o DataFrame diretamente na tabela Delta.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_fqn)
    )


def validate_dim_date(spark: SparkSession, dim_date_fqn: str) -> int:
    """
    Valida se a dimensao de datas possui a cardinalidade esperada.

    Args:
        spark: Sessao Spark ativa.
        dim_date_fqn: Nome totalmente qualificado da `dim_date`.

    Returns:
        Quantidade de linhas presentes na dimensao de datas.
    """
    rows_date = spark.table(dim_date_fqn).count()
    assert rows_date == 151, f"dim_date should have 151 rows, found {rows_date}"
    return rows_date


def summarize_dimensions(spark: SparkSession, context: dict[str, str]) -> dict[str, int | str]:
    """
    Resume o volume final gravado nas dimensoes Gold.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_gold_dimension_context`.

    Returns:
        Dicionario com contagens e FQNs das dimensoes publicadas.
    """
    rows_date = spark.table(context["dim_date_fqn"]).count()
    rows_vendor = spark.table(context["dim_vendor_fqn"]).count()
    print(f"dim_date: {rows_date} rows")
    print(f"dim_vendor: {rows_vendor} rows")
    return {
        "dim_date_rows": rows_date,
        "dim_vendor_rows": rows_vendor,
        "dim_date_fqn": context["dim_date_fqn"],
        "dim_vendor_fqn": context["dim_vendor_fqn"],
    }
