-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Respostas do Case — NYC Taxi (Jan–Mai 2023)
-- MAGIC Tabelas: `ifood.taxi_nyc_gold.fact_trip` + `dim_date` + `dim_vendor`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1: Média mensal de total_amount (yellow taxis)
-- MAGIC > "Qual a média de valor total (total_amount) recebido em um mês
-- MAGIC > considerando todos os **yellow** táxis da frota?"

-- COMMAND ----------

SELECT
    d.year,
    d.month,
    ROUND(AVG(f.total_amount), 2) AS avg_total_amount,
    COUNT(*)                      AS trip_count
FROM ifood.taxi_nyc_gold.fact_trip f
JOIN ifood.taxi_nyc_gold.dim_date  d ON f.date_key = d.date_key
WHERE f.taxi_type = 'yellow'
  AND d.year = 2023
  AND d.month BETWEEN 1 AND 5
GROUP BY d.year, d.month
ORDER BY d.month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2: Média de passenger_count por hora (maio/2023, todos os táxis)
-- MAGIC > "Qual a média de passageiros (passenger_count) por cada hora do dia
-- MAGIC > que pegaram táxi no mês de maio considerando todos os táxis da frota?"

-- COMMAND ----------

SELECT
    HOUR(f.pickup_datetime)           AS pickup_hour,
    ROUND(AVG(f.passenger_count), 4)  AS avg_passengers,
    COUNT(*)                          AS trip_count
FROM ifood.taxi_nyc_gold.fact_trip f
JOIN ifood.taxi_nyc_gold.dim_date  d ON f.date_key = d.date_key
WHERE d.year  = 2023
  AND d.month = 5
GROUP BY HOUR(f.pickup_datetime)
ORDER BY pickup_hour

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1 (variante via vendor_daily_cumulative)
-- MAGIC Demonstra uso das tabelas cumulativas como fonte alternativa.

-- COMMAND ----------

SELECT
    YEAR(snapshot_date)  AS year,
    MONTH(snapshot_date) AS month,
    ROUND(
        SUM(amount_cumulative) / NULLIF(SUM(trips_cumulative), 0),
        2
    ) AS avg_total_amount_cum
FROM ifood.taxi_nyc_gold.vendor_daily_cumulative
WHERE snapshot_date IN (
    SELECT MAX(snapshot_date)
    FROM ifood.taxi_nyc_gold.vendor_daily_cumulative
    GROUP BY YEAR(snapshot_date), MONTH(snapshot_date)
)
GROUP BY YEAR(snapshot_date), MONTH(snapshot_date)
ORDER BY month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2 (variante via hourly_passenger_cumulative)

-- COMMAND ----------

SELECT
    pickup_hour,
    ROUND(avg_passengers, 4) AS avg_passengers
FROM ifood.taxi_nyc_gold.hourly_passenger_cumulative
WHERE snapshot_date = (
    SELECT MAX(snapshot_date)
    FROM ifood.taxi_nyc_gold.hourly_passenger_cumulative
    WHERE YEAR(snapshot_date) = 2023 AND MONTH(snapshot_date) = 5
)
ORDER BY pickup_hour
