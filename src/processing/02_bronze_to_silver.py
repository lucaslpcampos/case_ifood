# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Bronze → Silver (DQ + MERGE)
# MAGIC Aplica regras de qualidade, normaliza colunas (tpep/lpep → pickup/dropoff_datetime),
# MAGIC gera `_trip_sk` (sha256) e faz MERGE idempotente na Silver.
# MAGIC
# MAGIC **Genérico:** recebe `taxi_type` (yellow/green) via widget.
# MAGIC Tabela criada com DDL explícito + **Liquid Clustering**.

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %run ../common/quality

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col

# COMMAND ----------

dbutils.widgets.text("taxi_type", "yellow")
dbutils.widgets.text("catalog", "ifood")

taxi_type = dbutils.widgets.get("taxi_type")
catalog = dbutils.widgets.get("catalog")

taxi_cfg = TAXI_CONFIGS[taxi_type]
bronze_fqn = get_fqn(catalog, SCHEMA_BRONZE, taxi_cfg["bronze_table"])
silver_fqn = get_fqn(catalog, SCHEMA_SILVER, taxi_cfg["silver_table"])
quarantine_fqn = get_fqn(catalog, SCHEMA_SILVER, taxi_cfg["quarantine_table"])

print(f"taxi_type={taxi_type}  bronze={bronze_fqn}  silver={silver_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — schema + tabelas silver

# COMMAND ----------

ensure_schemas(spark, catalog, [SCHEMA_SILVER])

spark.sql(silver_ddl(catalog, SCHEMA_SILVER, taxi_cfg["silver_table"]))
spark.sql(silver_quarantine_ddl(catalog, SCHEMA_SILVER, taxi_cfg["quarantine_table"]))

print(f"Tabelas {silver_fqn} e {quarantine_fqn} prontas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura + normalização de colunas

# COMMAND ----------

pickup_col = taxi_cfg["pickup_col"]
dropoff_col = taxi_cfg["dropoff_col"]

src = (
    spark.table(bronze_fqn)
    .withColumn("pickup_datetime", col(pickup_col).cast("timestamp"))
    .withColumn("dropoff_datetime", col(dropoff_col).cast("timestamp"))
    .withColumn("VendorID", col("VendorID").cast("int"))
    .withColumn("passenger_count", col("passenger_count").cast("int"))
    .withColumn("total_amount", col("total_amount").cast("decimal(10,2)"))
)

rows_in = src.count()
print(f"Bronze lido: {rows_in:,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicar regras de DQ (clean + quarantine)

# COMMAND ----------

clean, quarantine = apply_silver_rules(src)

rows_clean = clean.count()
rows_quarantine = quarantine.count()
print(f"Clean: {rows_clean:,}  Quarantine: {rows_quarantine:,}  ({rows_quarantine / max(rows_in, 1) * 100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gerar surrogate key + projetar schema silver

# COMMAND ----------

silver = (
    clean.select(
        col("VendorID"),
        col("passenger_count"),
        col("total_amount"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("_ingestion_ts"),
    )
    .withColumn("_trip_sk", trip_surrogate_key())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE idempotente na Silver

# COMMAND ----------

tgt = DeltaTable.forName(spark, silver_fqn)
(
    tgt.alias("t")
    .merge(silver.alias("s"), "t._trip_sk = s._trip_sk")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append quarantine

# COMMAND ----------

if rows_quarantine > 0:
    (
        quarantine.select(
            col("VendorID"),
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

rows_silver = spark.table(silver_fqn).count()
print(f"Silver {silver_fqn}: {rows_silver:,} linhas")

dup_check = spark.sql(f"""
    SELECT _trip_sk, COUNT(*) AS n
    FROM {silver_fqn}
    GROUP BY _trip_sk
    HAVING n > 1
""").count()
assert dup_check == 0, f"FALHA: {dup_check} _trip_sk duplicados em {silver_fqn}"

print(f"0 duplicatas confirmado. Quarantine: {rows_quarantine:,} linhas.")
dbutils.notebook.exit(f"OK rows={rows_silver} quarantine={rows_quarantine}")
