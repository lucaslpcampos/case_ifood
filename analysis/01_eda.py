# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # EDA — NYC Yellow & Green Taxi (Jan–Mai 2023)
# MAGIC Análise exploratória sobre as tabelas Silver e Gold.

# COMMAND ----------

# MAGIC %run ../src/common/config

# COMMAND ----------

dbutils.widgets.text("catalog", "ifood")
catalog = dbutils.widgets.get("catalog")

yellow_silver = f"{catalog}.{SCHEMA_SILVER}.yellow_trips"
green_silver = f"{catalog}.{SCHEMA_SILVER}.green_trips"
fact_trip = f"{catalog}.{SCHEMA_GOLD}.fact_trip"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Volumetria por mês e taxi_type

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        taxi_type,
        date_format(pickup_datetime, 'yyyy-MM') AS year_month,
        COUNT(*) AS trips
    FROM {fact_trip}
    GROUP BY taxi_type, date_format(pickup_datetime, 'yyyy-MM')
    ORDER BY taxi_type, year_month
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Distribuição de passenger_count

# COMMAND ----------

display(spark.sql(f"""
    SELECT passenger_count, COUNT(*) AS n
    FROM {fact_trip}
    GROUP BY passenger_count
    ORDER BY passenger_count
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Outliers de total_amount

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        taxi_type,
        MIN(total_amount)                    AS min_amt,
        MAX(total_amount)                    AS max_amt,
        ROUND(AVG(total_amount), 2)          AS avg_amt,
        ROUND(PERCENTILE(total_amount, 0.5), 2)  AS median_amt,
        ROUND(PERCENTILE(total_amount, 0.99), 2) AS p99_amt
    FROM {fact_trip}
    GROUP BY taxi_type
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quarantine summary (yellow)

# COMMAND ----------

display(spark.sql(f"""
    SELECT _rejection_reasons, COUNT(*) AS n
    FROM {catalog}.{SCHEMA_SILVER}.yellow_trips_quarantine
    GROUP BY _rejection_reasons
    ORDER BY n DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Quarantine summary (green)

# COMMAND ----------

display(spark.sql(f"""
    SELECT _rejection_reasons, COUNT(*) AS n
    FROM {catalog}.{SCHEMA_SILVER}.green_trips_quarantine
    GROUP BY _rejection_reasons
    ORDER BY n DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Trips por hora do dia (todas as frotas)

# COMMAND ----------

display(spark.sql(f"""
    SELECT HOUR(pickup_datetime) AS pickup_hour, COUNT(*) AS trips
    FROM {fact_trip}
    GROUP BY HOUR(pickup_datetime)
    ORDER BY pickup_hour
"""))
