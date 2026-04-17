# iFood Case — NYC Taxi Pipeline

Pipeline de dados em **Medallion 4-camadas** (Landing → Bronze → Silver → Gold) sobre Databricks Free Edition + Unity Catalog. Ingere yellow e green taxi trips da TLC (Jan–Mai/2023), aplica data quality com quarantine, materializa star schema + cumulative tables e responde:

- **Q1** — Média mensal de `total_amount` considerando os yellow taxis.
- **Q2** — Média de `passenger_count` por hora-do-dia em Maio/2023 considerando todos os taxis.

---

## Arquitetura

```
TLC CloudFront ──► Landing (UC Volume)
                       │
              ┌────────┴────────┐
              ▼                 ▼
        Auto Loader        Auto Loader
        (yellow)            (green)
              │                 │
              ▼                 ▼
        Bronze.yellow      Bronze.green      (Delta append)
              │                 │
              ▼                 ▼
        Silver.yellow      Silver.green      (DQ + MERGE _trip_sk)
              │                 │
              └────────┬────────┘
                       ▼
                    Gold Layer
              ┌────────┼────────┐
              ▼        ▼        ▼
          dim_date  dim_vendor  fact_trip (MERGE, yellow+green unificados)
                       │
              ┌────────┴────────┐
              ▼                 ▼
     vendor_daily_cum    hourly_passenger_cum
     (FULL OUTER JOIN)   (FULL OUTER JOIN)
```

Yellow e green correm **em paralelo** na ingestion/silver, convergem no Gold.

---

## Estrutura do Repositório

```
src/
  common/           config.py, schemas.py, quality.py, utils.py
  ingestion/        00_download_landing.py, 01_landing_to_bronze.py
  processing/       02_bronze_to_silver.py, 03_gold_dimensions.py,
                    04_gold_facts.py, 05_gold_cumulative.py
analysis/           01_eda.py, 02_case_answers.sql
resources/          workflow.yaml (Lakeflow Job, 9-task DAG)
databricks.yml      Asset Bundle
```

Todos os `.py` em `src/` são **notebooks Databricks** (`# Databricks notebook source`).
Notebooks genéricos recebem `taxi_type` (yellow/green) via **widgets** parametrizados pelo Job.

---

## Decisões Técnicas

| Decisão | Justificativa |
|---|---|
| **Medallion 4-layer** | Landing separada do Bronze para auditoria + reprocessamento |
| **Notebooks genéricos** | Mesmo código processa yellow e green via `taxi_type` widget |
| **Auto Loader `availableNow`** | Idempotência via checkpoint, incremental nativo |
| **MERGE por `_trip_sk` (sha256)** | Idempotência total, 0 duplicatas |
| **DDL explícito + Liquid Clustering** | Tabelas com schema declarado e `CLUSTER BY` para performance |
| **Delta Constraints (Gold)** | DQ enforcement nativo na escrita (`total_amount >= 0`, `passenger_count > 0`) |
| **Gold em Spark SQL** | Transformações via `spark.sql()` — mais legível para stakeholders |
| **Cumulative Table Design** | Running totals O(delta) via FULL OUTER JOIN D-1 ⟕ delta D |
| **Parametrização via plataforma** | Job parameters → widgets — sem config Python pesado |

---

## Setup — Databricks Free Edition

### 1. Workspace

Criar workspace em [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition).
Unity Catalog já vem habilitado.

### 2. Deploy via Asset Bundle

```bash
pip install databricks-cli
databricks configure --token
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run taxi_nyc_pipeline -t dev
```

### 3. Execução manual

Suba os notebooks em `src/` para o workspace e execute em sequência, passando os widgets:
- `taxi_type` = `yellow` ou `green`
- `catalog` = `ifood`

---

## Plano B — Internet bloqueada na Free Edition

Se o download falhar:

1. Baixe localmente:
   ```bash
   for type in yellow green; do
     for m in 01 02 03 04 05; do
       curl -O "https://d37ci6vzurychx.cloudfront.net/trip-data/${type}_tripdata_2023-${m}.parquet"
     done
   done
   ```

2. Suba ao Volume:
   ```bash
   for type in yellow green; do
     for m in 01 02 03 04 05; do
       databricks fs cp "${type}_tripdata_2023-${m}.parquet" \
         "dbfs:/Volumes/ifood/taxi_nyc_landing/raw/${type}/"
     done
   done
   ```

3. Execute o pipeline a partir do step 01 (landing_to_bronze).

---

## Data Quality

| Camada | Mecanismo | Regras |
|---|---|---|
| **Silver** | `quality.py` (clean + quarantine split) | `passenger_count > 0`, `dropoff > pickup`, `total_amount >= 0`, nulls |
| **Gold** | Delta Constraints (enforcement na escrita) | `total_amount >= 0`, `passenger_count > 0`, `duration_seconds >= 0` |
| **Gold** | Assertions nos notebooks | FK integrity (fact → dim_date), 0 duplicatas |

---

## Autor

Lucas Pereira Campos — case iFood (Data Architect).
