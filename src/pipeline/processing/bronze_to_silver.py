from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from common.config import (
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    STUDY_END_EXCLUSIVE,
    STUDY_START_DATE,
    TAXI_CONFIGS,
    get_fqn,
)
from common.quality import apply_silver_rules
from common.schemas import apply_table_comments, render_ddl
from common.utils import ensure_schemas, trip_surrogate_key


def build_silver_context(taxi_type: str, catalog: str) -> dict[str, object]:
    """
    Monta o contexto operacional usado pelo processamento Bronze para Silver.

    Args:
        taxi_type: Tipo de taxi configurado em `TAXI_CONFIGS`.
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        Dicionario com configuracoes, tabelas e FQNs usados pelo job.
    """
    taxi_cfg = TAXI_CONFIGS[taxi_type]
    return {
        "taxi_type": taxi_type,
        "catalog": catalog,
        "taxi_cfg": taxi_cfg,
        "bronze_fqn": get_fqn(catalog, SCHEMA_BRONZE, taxi_cfg["bronze_table"]),
        "silver_fqn": get_fqn(catalog, SCHEMA_SILVER, taxi_cfg["silver_table"]),
        "quarantine_fqn": get_fqn(catalog, SCHEMA_SILVER, taxi_cfg["quarantine_table"]),
    }


def ensure_silver_tables(spark: SparkSession, context: dict[str, object]) -> None:
    """
    Garante a existencia das tabelas Silver principal e de quarentena.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_silver_context`.

    Returns:
        None. A funcao executa DDLs para preparar as tabelas Silver.
    """
    taxi_cfg = context["taxi_cfg"]
    ensure_schemas(spark, context["catalog"], [SCHEMA_SILVER])
    spark.sql(
        render_ddl(
            taxi_cfg["silver_schema_key"],
            catalog=context["catalog"],
            schema=SCHEMA_SILVER,
            table=taxi_cfg["silver_table"],
        )
    )
    apply_table_comments(
        spark,
        taxi_cfg["silver_schema_key"],
        catalog=context["catalog"],
        schema=SCHEMA_SILVER,
        table=taxi_cfg["silver_table"],
    )
    spark.sql(
        render_ddl(
            taxi_cfg["quarantine_schema_key"],
            catalog=context["catalog"],
            schema=SCHEMA_SILVER,
            table=taxi_cfg["quarantine_table"],
        )
    )
    apply_table_comments(
        spark,
        taxi_cfg["quarantine_schema_key"],
        catalog=context["catalog"],
        schema=SCHEMA_SILVER,
        table=taxi_cfg["quarantine_table"],
    )


def build_silver_source_df(spark: SparkSession, context: dict[str, object]) -> DataFrame:
    """
    Le a Bronze, padroniza tipos e prepara a chave de merge da Silver.

    Args:
        spark: Sessao Spark ativa.
        context: Contexto operacional retornado por `build_silver_context`.

    Returns:
        DataFrame com colunas padronizadas para validacao e merge na Silver.
    """
    taxi_cfg = context["taxi_cfg"]
    pickup_col = taxi_cfg["pickup_col"]
    dropoff_col = taxi_cfg["dropoff_col"]
    taxi_type = context["taxi_type"]

    return (
        spark.table(context["bronze_fqn"])
        .withColumn("pickup_datetime", col(pickup_col).cast("timestamp"))
        .withColumn("dropoff_datetime", col(dropoff_col).cast("timestamp"))
        .withColumn("VendorID", col("VendorID").cast("int"))
        .withColumn("passenger_count", col("passenger_count").cast("int"))
        .withColumn("total_amount", col("total_amount").cast("decimal(10,2)"))
        .withColumn("tripsk", trip_surrogate_key(taxi_type))
    )


def summarize_silver_source(source_df: DataFrame) -> int:
    """
    Conta e imprime a quantidade de registros lidos da Bronze.

    Args:
        source_df: DataFrame de origem preparado para a Silver.

    Returns:
        Quantidade total de linhas lidas da Bronze.
    """
    rows_in = source_df.count()
    print(f"Bronze read: {rows_in:,} rows")
    return rows_in


def split_silver_clean_and_quarantine(source_df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Separa registros validos e quarentenados conforme o ruleset Silver.

    Args:
        source_df: DataFrame de origem preparado para a Silver.

    Returns:
        Tupla com DataFrame limpo e DataFrame de quarentena.
    """
    return apply_silver_rules(
        source_df,
        STUDY_START_DATE,
        STUDY_END_EXCLUSIVE,
    )


def summarize_silver_quality(
    rows_in: int,
    clean_df: DataFrame,
    quarantine_df: DataFrame,
) -> dict[str, float]:
    """
    Conta e imprime o volume de registros limpos e quarentenados.

    Args:
        rows_in: Quantidade total de linhas lidas da Bronze.
        clean_df: DataFrame com registros aprovados no ruleset Silver.
        quarantine_df: DataFrame com registros rejeitados no ruleset Silver.

    Returns:
        Dicionario com contagens e percentual de quarentena.
    """
    rows_clean = clean_df.count()
    rows_quarantine = quarantine_df.count()
    quarantine_pct = rows_quarantine / max(rows_in, 1) * 100
    print(
        f"Clean: {rows_clean:,}  Quarantine: {rows_quarantine:,}  "
        f"({quarantine_pct:.1f}%)"
    )
    return {
        "rows_clean": rows_clean,
        "rows_quarantine": rows_quarantine,
        "quarantine_pct": quarantine_pct,
    }


def project_silver_df(clean_df: DataFrame) -> DataFrame:
    """
    Projeta apenas as colunas persistidas na tabela Silver principal.

    Args:
        clean_df: DataFrame com registros aprovados no ruleset Silver.

    Returns:
        DataFrame final usado como source do merge na Silver.
    """
    return (
        clean_df.select(
            col("VendorID"),
            col("passenger_count"),
            col("total_amount"),
            col("pickup_datetime"),
            col("dropoff_datetime"),
            col("tripsk"),
            col("_ingestion_ts"),
        )
    )


def find_duplicate_merge_keys(source_df: DataFrame, merge_key: str = "tripsk") -> DataFrame:
    """
    Identifica valores duplicados da chave de merge na origem da Silver.

    Args:
        source_df: DataFrame que sera usado como source do merge.
        merge_key: Nome da coluna usada como chave de merge.

    Returns:
        DataFrame com as chaves duplicadas e sua contagem.
    """
    return (
        source_df.groupBy(merge_key)
        .count()
        .filter(col("count") > 1)
    )


def validate_merge_source_uniqueness(
    source_df: DataFrame,
    merge_key: str = "tripsk",
    sample_size: int = 10,
) -> dict[str, object]:
    """
    Valida se a origem do merge possui uma linha por chave.

    Args:
        source_df: DataFrame que sera usado como source do merge.
        merge_key: Nome da coluna usada como chave de merge.
        sample_size: Quantidade maxima de chaves duplicadas exibidas no erro.

    Returns:
        Dicionario com contagem de duplicidades e amostra vazia quando nao ha conflito.
    """
    duplicate_keys_df = find_duplicate_merge_keys(source_df, merge_key)
    duplicate_key_count = duplicate_keys_df.count()

    if duplicate_key_count == 0:
        return {
            "duplicate_key_count": 0,
            "sample": [],
        }

    sample_rows = (
        duplicate_keys_df.orderBy(col("count").desc(), col(merge_key).asc())
        .limit(sample_size)
        .collect()
    )
    sample = [
        {
            merge_key: row[merge_key],
            "count": row["count"],
        }
        for row in sample_rows
    ]
    sample_text = ", ".join(
        f"{row[merge_key]} ({row['count']} rows)"
        for row in sample
    )

    raise ValueError(
        f"Merge source contains {duplicate_key_count} duplicated {merge_key} values. "
        "This usually indicates replay in Bronze or an insufficient merge key design. "
        f"Sample duplicated keys: {sample_text}"
    )


def merge_silver_df(spark: SparkSession, silver_fqn: str, silver_df: DataFrame) -> None:
    """
    Executa o merge incremental da origem limpa na tabela Silver.

    Args:
        spark: Sessao Spark ativa.
        silver_fqn: Nome totalmente qualificado da tabela Silver de destino.
        silver_df: DataFrame limpo projetado para persistencia.

    Returns:
        None. A funcao executa o merge diretamente na tabela Delta.
    """
    target_table = DeltaTable.forName(spark, silver_fqn)
    (
        target_table.alias("target")
        .merge(silver_df.alias("source"), "target.tripsk = source.tripsk")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def append_quarantine_df(quarantine_fqn: str, quarantine_df: DataFrame) -> int:
    """
    Persiste os registros rejeitados na tabela de quarentena da Silver.

    Args:
        quarantine_fqn: Nome totalmente qualificado da tabela de quarentena.
        quarantine_df: DataFrame com os registros rejeitados pelo ruleset Silver.

    Returns:
        Quantidade de linhas gravadas em quarentena na execucao atual.
    """
    rows_quarantine = quarantine_df.count()
    if rows_quarantine > 0:
        (
            quarantine_df.select(
                col("VendorID").cast("int"),
                col("passenger_count").cast("double"),
                col("total_amount").cast("double"),
                col("pickup_datetime"),
                col("dropoff_datetime"),
                col("_ingestion_ts"),
                col("_quarantined_at"),
                col("_rejection_reasons"),
            )
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quarantine_fqn)
        )
    return rows_quarantine


def validate_silver(spark: SparkSession, silver_fqn: str) -> dict[str, int]:
    """
    Verifica se a tabela Silver persistida nao contem chaves duplicadas.

    Args:
        spark: Sessao Spark ativa.
        silver_fqn: Nome totalmente qualificado da tabela Silver validada.

    Returns:
        Dicionario com quantidade de linhas e duplicidades encontradas.
    """
    rows_silver = spark.table(silver_fqn).count()
    duplicated_keys = spark.sql(
        f"""
        SELECT tripsk, COUNT(*) AS n
        FROM {silver_fqn}
        GROUP BY tripsk
        HAVING n > 1
        """
    ).count()
    assert duplicated_keys == 0, (
        f"FAIL: {duplicated_keys} duplicated tripsk values in {silver_fqn}"
    )
    return {
        "rows": rows_silver,
        "duplicated_keys": duplicated_keys,
    }
