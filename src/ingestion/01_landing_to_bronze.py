# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Landing → Bronze (Auto Loader)
# MAGIC Lê parquets do UC Volume via Auto Loader (`cloudFiles`) com `trigger=availableNow`.
# MAGIC
# MAGIC **Genérico:** recebe `taxi_type` (yellow/green) via widget.
# MAGIC Tabela criada com DDL explícito + **Liquid Clustering**.

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit

# COMMAND ----------

dbutils.widgets.text("taxi_type", "yellow")
dbutils.widgets.text("catalog", "ifood")

taxi_type = dbutils.widgets.get("taxi_type")
catalog = dbutils.widgets.get("catalog")

taxi_cfg = TAXI_CONFIGS[taxi_type]
landing_path = get_landing_path(catalog, taxi_type)
checkpoint_path = f"{get_checkpoint_root(catalog)}/{taxi_type}_bronze"
schema_loc = f"{get_checkpoint_root(catalog)}/{taxi_type}_bronze_schema"
bronze_fqn = get_fqn(catalog, SCHEMA_BRONZE, taxi_cfg["bronze_table"])

print(f"taxi_type={taxi_type}  landing={landing_path}  bronze={bronze_fqn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — schemas + tabela bronze

# COMMAND ----------

ensure_schemas(spark, catalog, [SCHEMA_BRONZE])

spark.sql(
    bronze_ddl(catalog, SCHEMA_BRONZE, taxi_cfg["bronze_table"],
               taxi_cfg["pickup_col"], taxi_cfg["dropoff_col"])
)

print(f"Tabela {bronze_fqn} pronta.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader — ingestão incremental

# COMMAND ----------

batch_id = new_batch_id()

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", schema_loc)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load(landing_path)
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_batch_id", lit(batch_id))
)

query = (
    df.writeStream.format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(bronze_fqn)
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

row_count = spark.table(bronze_fqn).count()
print(f"Bronze {bronze_fqn}: {row_count:,} linhas  batch_id={batch_id}")
dbutils.notebook.exit(f"OK rows={row_count}")
