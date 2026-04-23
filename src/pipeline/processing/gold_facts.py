from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from common.config import (
    FACT_SOURCE_TAXI_TYPES,
    SCHEMA_GOLD,
    SCHEMA_SILVER,
    STUDY_END_EXCLUSIVE,
    STUDY_START_DATE,
    TAXI_CONFIGS,
    get_fqn,
)
from common.quality import apply_gold_constraints
from common.schemas import apply_table_comments, render_ddl
from common.sql import build_fact_select_query
from common.utils import ensure_schemas


def build_gold_fact_context(catalog: str) -> dict[str, object]:
    """
    Monta o contexto operacional usado pela carga da fato Gold.

    Args:
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        Dicionario com FQNs de destino, dimensoes e entradas Silver esperadas.
    """
    gold_schema = f"{catalog}.{SCHEMA_GOLD}"
    silver_inputs = {
        taxi_type: get_fqn(catalog, SCHEMA_SILVER, TAXI_CONFIGS[taxi_type]["silver_table"])
        for taxi_type in FACT_SOURCE_TAXI_TYPES
    }
    return {
        "catalog": catalog,
        "gold_schema": gold_schema,
        "fact_fqn": f"{gold_schema}.fact_trip",
        "dim_date_fqn": f"{gold_schema}.dim_date",
        "dim_vendor_fqn": f"{gold_schema}.dim_vendor",
        "silver_inputs": silver_inputs,
    }


def ensure_gold_fact_table(spark: SparkSession, context: dict[str, object]) -> None:
    """
    Garante a existencia da fato Gold e aplica suas constraints.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_gold_fact_context`.

    Returns:
        None. A funcao executa DDLs e constraints na tabela de destino.
    """
    ensure_schemas(spark, context["catalog"], [SCHEMA_GOLD])
    spark.sql(render_ddl("fact_trip", catalog=context["catalog"], schema=SCHEMA_GOLD))
    apply_table_comments(spark, "fact_trip", catalog=context["catalog"], schema=SCHEMA_GOLD)
    apply_gold_constraints(spark, context["fact_fqn"], "fact_trip")


def build_fact_union_parts(spark: SparkSession, context: dict[str, object]) -> list[str]:
    """
    Monta as queries SQL que alimentam a fato a partir das tabelas Silver.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_gold_fact_context`.

    Returns:
        Lista de selects SQL prontos para serem unidos por `UNION ALL`.
    """
    missing_tables = [
        silver_fqn
        for silver_fqn in context["silver_inputs"].values()
        if not spark.catalog.tableExists(silver_fqn)
    ]
    if missing_tables:
        missing_list = ", ".join(sorted(missing_tables))
        raise ValueError(f"Missing required silver tables: {missing_list}")

    union_parts = []
    for taxi_type, silver_fqn in context["silver_inputs"].items():
        union_parts.append(
            build_fact_select_query(
                silver_fqn,
                taxi_type,
                STUDY_START_DATE,
                STUDY_END_EXCLUSIVE,
            )
        )
    return union_parts


def build_fact_df(spark: SparkSession, union_parts: list[str]) -> DataFrame:
    """
    Construi o DataFrame final da fato a partir das queries das origens Silver.

    Args:
        spark: Sessao Spark ativa.
        union_parts: Lista de selects SQL retornada por `build_fact_union_parts`.

    Returns:
        DataFrame pronto para merge na `fact_trip`.
    """
    assert union_parts, "No silver tables available"
    return spark.sql(" UNION ALL ".join(union_parts))


def summarize_fact_source(fact_df: DataFrame) -> int:
    """
    Conta e imprime o volume de linhas que entrara no merge da fato.

    Args:
        fact_df: DataFrame de origem preparado para a `fact_trip`.

    Returns:
        Quantidade de linhas que participarao do merge.
    """
    rows_for_merge = fact_df.count()
    print(f"Rows for MERGE: {rows_for_merge:,}")
    return rows_for_merge


def merge_fact_df(spark: SparkSession, fact_fqn: str, fact_df: DataFrame) -> None:
    """
    Executa o merge incremental do DataFrame da fato na tabela Gold.

    Args:
        spark: Sessao Spark ativa.
        fact_fqn: Nome totalmente qualificado da tabela `fact_trip`.
        fact_df: DataFrame consolidado usado como source do merge.

    Returns:
        None. A funcao executa o merge diretamente na tabela Delta.
    """
    target_table = DeltaTable.forName(spark, fact_fqn)
    (
        target_table.alias("target")
        .merge(fact_df.alias("source"), "target.tripsk = source.tripsk")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def validate_fact_dimensions(spark: SparkSession, context: dict[str, object]) -> dict[str, int]:
    """
    Valida se toda linha da fato possui correspondencia nas dimensoes principais.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_gold_fact_context`.

    Returns:
        Dicionario com total de linhas da fato e contagem de orfaos por dimensao.
    """
    rows_fact = spark.table(context["fact_fqn"]).count()
    orphan_dates = spark.sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {context['fact_fqn']} f
        LEFT ANTI JOIN {context['dim_date_fqn']} d ON f.date_key = d.date_key
        """
    ).collect()[0]["n"]
    orphan_vendors = spark.sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {context['fact_fqn']} f
        LEFT ANTI JOIN {context['dim_vendor_fqn']} v ON f.vendor_id = v.vendor_id
        """
    ).collect()[0]["n"]

    assert orphan_dates == 0, (
        f"{orphan_dates} rows in fact_trip are missing a matching dim_date row"
    )
    assert orphan_vendors == 0, (
        f"{orphan_vendors} rows in fact_trip are missing a matching dim_vendor row"
    )
    return {
        "rows": rows_fact,
        "orphan_dates": orphan_dates,
        "orphan_vendors": orphan_vendors,
    }


def summarize_fact(metrics: dict[str, int]) -> None:
    """
    Imprime um resumo final das metricas de carga da fato.

    Args:
        metrics: Dicionario retornado por `validate_fact_dimensions`.

    Returns:
        None. A funcao apenas imprime o resumo da execucao.
    """
    print(f"fact_trip: {metrics['rows']:,} rows")
    print(
        f"Orphans dim_date: {metrics['orphan_dates']}  "
        f"dim_vendor: {metrics['orphan_vendors']}"
    )
