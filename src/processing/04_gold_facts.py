# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 — Gold: fact_trip (Yellow + Green unificados)
# MAGIC Unifica yellow e green trips da Silver em uma fact table única.
# MAGIC Transformação principal em **Spark SQL**, MERGE e constraints via PySpark/Delta API.
# MAGIC
# MAGIC **Liquid Clustering** por `pickup_datetime`. **Delta Constraints** para DQ enforcement.

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

# COMMAND ----------

dbutils.widgets.text("catalog", "ifood")
catalog = dbutils.widgets.get("catalog")

gold_schema = f"{catalog}.{SCHEMA_GOLD}"
fact_fqn = f"{gold_schema}.fact_trip"
dim_date_fqn = f"{gold_schema}.dim_date"
dim_vendor_fqn = f"{gold_schema}.dim_vendor"

yellow_silver = get_fqn(catalog, SCHEMA_SILVER, TAXI_CONFIGS["yellow"]["silver_table"])
green_silver = get_fqn(catalog, SCHEMA_SILVER, TAXI_CONFIGS["green"]["silver_table"])

print(f"fact_trip={fact_fqn}  yellow={yellow_silver}  green={green_silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — tabela + constraints

# COMMAND ----------

spark.sql(FACT_TRIP_DDL.format(catalog=catalog, schema=SCHEMA_GOLD))
apply_gold_constraints(spark, fact_fqn, "fact_trip")

print(f"Tabela {fact_fqn} pronta com constraints.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformação SQL — unifica yellow + green

# COMMAND ----------

yellow_exists = spark.catalog.tableExists(yellow_silver)
green_exists = spark.catalog.tableExists(green_silver)

union_parts = []
if yellow_exists:
    union_parts.append(f"""
        SELECT
            s._trip_sk,
            INT(date_format(s.pickup_datetime, 'yyyyMMdd')) AS date_key,
            s.VendorID   AS vendor_id,
            s.passenger_count,
            s.total_amount,
            s.pickup_datetime,
            s.dropoff_datetime,
            BIGINT(unix_timestamp(s.dropoff_datetime) - unix_timestamp(s.pickup_datetime)) AS duration_seconds,
            'yellow'     AS taxi_type,
            s._ingestion_ts
        FROM {yellow_silver} s
    """)

if green_exists:
    union_parts.append(f"""
        SELECT
            s._trip_sk,
            INT(date_format(s.pickup_datetime, 'yyyyMMdd')) AS date_key,
            s.VendorID   AS vendor_id,
            s.passenger_count,
            s.total_amount,
            s.pickup_datetime,
            s.dropoff_datetime,
            BIGINT(unix_timestamp(s.dropoff_datetime) - unix_timestamp(s.pickup_datetime)) AS duration_seconds,
            'green'      AS taxi_type,
            s._ingestion_ts
        FROM {green_silver} s
    """)

assert union_parts, "Nenhuma tabela silver disponível"

unified_sql = " UNION ALL ".join(union_parts)
df_fact = spark.sql(unified_sql)

print(f"Linhas para MERGE: {df_fact.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MERGE na fact_trip

# COMMAND ----------

tgt = DeltaTable.forName(spark, fact_fqn)
(
    tgt.alias("t")
    .merge(df_fact.alias("s"), "t._trip_sk = s._trip_sk")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação — integridade referencial

# COMMAND ----------

rows_fact = spark.table(fact_fqn).count()

orphan_dates = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {fact_fqn} f
    LEFT ANTI JOIN {dim_date_fqn} d ON f.date_key = d.date_key
""").collect()[0]["n"]

orphan_vendors = spark.sql(f"""
    SELECT COUNT(*) AS n
    FROM {fact_fqn} f
    LEFT ANTI JOIN {dim_vendor_fqn} v ON f.vendor_id = v.vendor_id
""").collect()[0]["n"]

print(f"fact_trip: {rows_fact:,} linhas")
print(f"Órfãos dim_date: {orphan_dates}  dim_vendor: {orphan_vendors}")

assert orphan_dates == 0, f"{orphan_dates} linhas fact_trip sem dim_date correspondente"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribuição por taxi_type

# COMMAND ----------

display(spark.sql(f"""
    SELECT taxi_type, COUNT(*) AS trips, ROUND(AVG(total_amount), 2) AS avg_amount
    FROM {fact_fqn}
    GROUP BY taxi_type
"""))

dbutils.notebook.exit(f"OK rows={rows_fact} orphan_dates={orphan_dates} orphan_vendors={orphan_vendors}")
