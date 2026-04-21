-- Databricks notebook source

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Respostas do Case - NYC Taxi (Jan-Mai 2023)
-- MAGIC Tabelas: `ifood.taxi_nyc_gold.fact_trip`, `agg_trip_monthly_taxi`,
-- MAGIC `agg_trip_hourly_daily`, `dim_date` e `dim_vendor`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1: Media mensal de total_amount (yellow taxis)
-- MAGIC > "Qual a media de valor total (total_amount) recebido em um mes
-- MAGIC > considerando todos os **yellow** taxis da frota?"

-- COMMAND ----------

SELECT
    year,
    month,
    avg_total_amount,
    trip_count
FROM ifood.taxi_nyc_gold.agg_trip_monthly_taxi
WHERE taxi_type = 'yellow'
ORDER BY year, month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2: Media de passenger_count por hora (maio/2023, todos os taxis)
-- MAGIC > "Qual a media de passageiros (passenger_count) por cada hora do dia
-- MAGIC > que pegaram taxi no mes de maio considerando todos os taxis da frota?"

-- COMMAND ----------

SELECT
    a.pickup_hour,
    ROUND(
        CAST(SUM(a.passenger_count_sum) AS DOUBLE) / NULLIF(SUM(a.trip_count), 0),
        4
    ) AS avg_passengers,
    SUM(a.trip_count) AS trip_count
FROM ifood.taxi_nyc_gold.agg_trip_hourly_daily a
JOIN ifood.taxi_nyc_gold.dim_date d ON a.date_key = d.date_key
WHERE d.year = 2023
  AND d.month = 5
GROUP BY a.pickup_hour
ORDER BY a.pickup_hour

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q1 (validacao via fact_trip)
-- MAGIC Sanity check da tabela agregada mensal contra a fato transacional.

-- COMMAND ----------

SELECT
    d.year,
    d.month,
    ROUND(AVG(f.total_amount), 2) AS avg_total_amount,
    COUNT(*) AS trip_count
FROM ifood.taxi_nyc_gold.fact_trip f
JOIN ifood.taxi_nyc_gold.dim_date d ON f.date_key = d.date_key
WHERE f.taxi_type = 'yellow'
GROUP BY d.year, d.month
ORDER BY d.year, d.month

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q2 (validacao via fact_trip)

-- COMMAND ----------

SELECT
    HOUR(f.pickup_datetime) AS pickup_hour,
    ROUND(AVG(f.passenger_count), 4) AS avg_passengers,
    COUNT(*) AS trip_count
FROM ifood.taxi_nyc_gold.fact_trip f
JOIN ifood.taxi_nyc_gold.dim_date d ON f.date_key = d.date_key
WHERE d.year = 2023
  AND d.month = 5
GROUP BY HOUR(f.pickup_datetime)
ORDER BY pickup_hour
