# iFood Case - NYC Taxi Pipeline

Pipeline de dados em arquitetura Medallion com 4 camadas:
Landing -> Bronze -> Silver -> Gold.

A solucao ingere corridas yellow e green taxi da TLC (jan-mai/2023), aplica data quality com quarantine, materializa dimensoes, fato transacional e fatos agregados, e responde:

- **Q1** - Media mensal de `total_amount` considerando os yellow taxis.
- **Q2** - Media de `passenger_count` por hora do dia em maio/2023 considerando todos os taxis.

---

## Arquitetura

```text
TLC CloudFront -> Landing (UC Volume)
                    |
             +------+------+
             |             |
             v             v
      Auto Loader     Auto Loader
        (yellow)        (green)
             |             |
             v             v
      Bronze.yellow   Bronze.green
             |             |
             v             v
      Silver.yellow   Silver.green
             |             |
             +------+------+
                    |
                    v
                 Gold Layer
          +---------+---------+---------+
          |         |         |         |
          v         v         v         v
      dim_date  dim_vendor  fact_trip  agg_trip_monthly_taxi
                              |
                              v
                    agg_trip_hourly_daily
```

Yellow e green rodam em paralelo ate a Silver e convergem na Gold.

---

## Estrutura do Repositorio

```text
src/
  common/           modulos compartilhados (config, schemas, quality, utils)
  pipeline/         logica de negocio importavel/testavel localmente
    ingestion/      download_landing.py, landing_to_bronze.py
    processing/     bronze_to_silver.py, gold_dimensions.py,
                    gold_facts.py, gold_aggregates.py
  00_*.py           wrappers finos em formato source notebook Databricks
analysis/           scripts/notebooks com EDA e respostas do case
resources/          workflow.yaml com a DAG do job
databricks.yml      configuracao do bundle
requirements.txt    dependencias Python
```

Os arquivos numerados em `src/` continuam sendo notebooks Databricks em source format.
Eles apenas recebem widgets e chamam funcoes Python puras em `src/pipeline/`.

Esse arranjo preserva a estrutura pedida no case (`src/` + `analysis/`) e reduz o acoplamento a `%run`, facilitando testes locais, manutencao e revisao de codigo.

---

## Decisoes Tecnicas

| Decisao | Justificativa |
|---|---|
| **Medallion 4-layer** | Landing separada do Bronze para auditoria e reprocessamento |
| **Wrappers + modulos Python** | Notebooks finos para o Databricks, logica principal em modulos reutilizaveis |
| **Bronze permissiva** | Bronze absorve variacoes conhecidas de schema da fonte sem empurrar dados validos para `_rescued_data` |
| **Auto Loader `availableNow`** | Idempotencia via checkpoint, com processamento incremental nativo |
| **MERGE por `tripsk` (sha256)** | Idempotencia total com surrogate key baseada em atributos do registro |
| **DDL explicito + Liquid Clustering** | Tabelas com schema declarado e `CLUSTER BY` para performance |
| **Delta Constraints (Gold)** | Enforcement de regras criticas de qualidade na escrita |
| **Gold em Spark SQL** | Transformacoes legiveis para stakeholders e avaliadores |
| **Fatos agregados orientados a consumo** | Facilita responder o case sem abrir mao da `fact_trip` como base canonica |

---

## Setup - Databricks Free Edition

### 1. Workspace

Criar workspace em [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition).
Unity Catalog ja vem habilitado.

### 2. Deploy via Bundle

```bash
pip install databricks-cli
databricks configure --token
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run taxi_nyc_pipeline -t dev
```

### 3. Execucao manual

Execute os notebooks numerados em `src/` na sequencia abaixo:

- `src/00_download_landing.py`
- `src/01_landing_to_bronze.py`
- `src/02_bronze_to_silver.py`
- `src/03_gold_dimensions.py`
- `src/04_gold_facts.py`
- `src/05_gold_aggregates.py`

Parametros:

- `taxi_type` = `yellow` ou `green` para os steps 00, 01 e 02
- `catalog` = `ifood`

Observacoes:

- `src/common/` e `src/pipeline/` sao arquivos Python normais, importados pelos notebooks wrappers.
- `analysis/01_eda.py` adiciona `src/` ao `sys.path` para reutilizar a mesma configuracao compartilhada.

---

## Plano B - Internet bloqueada na Free Edition

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

3. Execute o pipeline a partir do step `01_landing_to_bronze`.

---

## Data Quality

| Camada | Mecanismo | Regras |
|---|---|---|
| **Bronze** | Auto Loader + projeção explicita | preserva o dado de origem com tipos amplos; `_rescued_data` fica reservado para drift inesperado |
| **Silver** | `quality.py` (clean + quarantine split) | `passenger_count > 0`, `dropoff > pickup`, `total_amount >= 0`, nulls obrigatorios |
| **Gold** | Delta Constraints | `total_amount >= 0`, `passenger_count > 0`, `duration_seconds >= 0` |
| **Gold** | Assertions nos modulos | integridade de dimensoes e ausencia de duplicatas |

---

## Desenvolvimento Local

A logica principal do pipeline agora esta concentrada em:

- `src/common/`
- `src/pipeline/ingestion/`
- `src/pipeline/processing/`

Com isso, a maior parte do codigo pode ser importada e validada localmente sem depender de `%run`.

---

## Autor

Lucas Pereira Campos - case iFood (Data Architect).
