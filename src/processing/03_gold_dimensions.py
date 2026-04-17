# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 — Gold: Dimensões (dim_date + dim_vendor)
# MAGIC Cria/substitui as tabelas de dimensão. Idempotente (overwrite determinístico).
# MAGIC Transformações em **Spark SQL** com orquestração PySpark.

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/schemas

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

dbutils.widgets.text("catalog", "ifood")
catalog = dbutils.widgets.get("catalog")

gold_schema = f"{catalog}.{SCHEMA_GOLD}"
dim_date_fqn = f"{gold_schema}.dim_date"
dim_vendor_fqn = f"{gold_schema}.dim_vendor"

print(f"catalog={catalog}  gold_schema={gold_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

ensure_schemas(spark, catalog, [SCHEMA_GOLD])

spark.sql(DIM_DATE_DDL.format(catalog=catalog, schema=SCHEMA_GOLD))
spark.sql(DIM_VENDOR_DDL.format(catalog=catalog, schema=SCHEMA_GOLD))

print("Tabelas dim_date e dim_vendor prontas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date (Jan–Mai 2023, 151 dias)

# COMMAND ----------

df_dim_date = spark.sql("""
    SELECT
        INT(date_format(full_date, 'yyyyMMdd'))    AS date_key,
        full_date,
        YEAR(full_date)                             AS year,
        QUARTER(full_date)                          AS quarter,
        MONTH(full_date)                            AS month,
        DAY(full_date)                              AS day,
        DAYOFWEEK(full_date)                        AS day_of_week,
        WEEKOFYEAR(full_date)                       AS week_of_year,
        CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN true ELSE false END AS is_weekend
    FROM (
        SELECT explode(
            sequence(to_date('2023-01-01'), to_date('2023-05-31'), interval 1 day)
        ) AS full_date
    )
""")

(
    df_dim_date.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(dim_date_fqn)
)

rows_date = spark.table(dim_date_fqn).count()
assert rows_date == 151, f"dim_date deveria ter 151 linhas, tem {rows_date}"
print(f"dim_date: {rows_date} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_vendor

# COMMAND ----------

df_dim_vendor = spark.sql("""
    SELECT vendor_id, vendor_name, vendor_short_name
    FROM VALUES
        (1, 'Creative Mobile Technologies, LLC', 'CMT'),
        (2, 'VeriFone Inc.', 'VeriFone'),
        (6, 'Myle Technologies Inc', 'Myle'),
        (7, 'Helix', 'Helix')
    AS t(vendor_id, vendor_name, vendor_short_name)
""")

(
    df_dim_vendor.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(dim_vendor_fqn)
)

rows_vendor = spark.table(dim_vendor_fqn).count()
print(f"dim_vendor: {rows_vendor} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação

# COMMAND ----------

display(spark.table(dim_date_fqn).limit(5))
display(spark.table(dim_vendor_fqn))

dbutils.notebook.exit(f"OK dim_date={rows_date} dim_vendor={rows_vendor}")
