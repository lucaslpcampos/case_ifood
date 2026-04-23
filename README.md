# iFood Case - NYC Taxi Pipeline

Pipeline de dados em arquitetura Medallion para corridas de taxi de NYC.
O projeto ingere dados `yellow` e `green` da TLC para o periodo de janeiro a maio de 2023, trata schema drift e data quality, e publica tabelas analiticas na Gold.

O pipeline responde as seguintes perguntas do case:

- Q1: media mensal de `total_amount` considerando os `yellow taxis`
- Q2: media de `passenger_count` por hora do dia em maio de 2023 considerando todos os taxis

## Overview

Arquitetura implementada:

```text
TLC CloudFront -> Landing -> Bronze -> Silver -> Gold
```

Camadas:

- `Landing`: recebimento bruto dos arquivos parquet
- `Bronze`: preservacao fiel da origem com tratamento de schema drift
- `Silver`: padronizacao, data quality e quarantine
- `Gold`: dimensoes, fato transacional e fatos agregados para consumo

Principais entregaveis:

- `dim_date`
- `dim_vendor`
- `fact_trip`
- `agg_trip_monthly_taxi`
- `agg_trip_hourly_daily`

## Estrutura do Repositorio

```text
src/
  common/           configuracoes, schemas, regras de qualidade e utils
  pipeline/         logica principal do pipeline
    ingestion/      download_landing.py, landing_to_bronze.py
    processing/     bronze_to_silver.py, gold_dimensions.py,
                    gold_facts.py, gold_aggregates.py
  00_*.py           notebooks wrappers em source format Databricks
analysis/           EDA e respostas do case
resources/          workflow do Databricks job
databricks.yml      configuracao do Databricks Asset Bundle
requirements.txt    dependencias Python
```

Os notebooks numerados em `src/` sao entrypoints finos para execucao no Databricks.
A logica principal fica em `src/pipeline/` e `src/common/`, o que reduz acoplamento a notebooks e facilita manutencao.

## Como Executar

### Pre-requisitos

- conta no Databricks Free Edition
- Unity Catalog habilitado
- Databricks CLI v0.218.0 ou superior
- Python com dependencias de `requirements.txt`

O projeto foi desenhado e testado pensando no Databricks Free Edition. Outros workspaces podem funcionar, mas as instrucoes abaixo assumem esse ambiente.

### 1. Configurar ambiente local

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Instalar e autenticar a Databricks CLI

Para comandos de `bundle`, use a Databricks CLI atual, nao o pacote legado `databricks-cli`.

No Windows, uma opcao simples e:

```bash
winget install Databricks.DatabricksCLI
databricks -v
```

#### Autenticacao recomendada

O modo recomendado pela Databricks e OAuth para usuario.
Se preferir seguir por token pessoal no workspace, use o fluxo abaixo.

#### Como gerar um token pessoal

No workspace Databricks:

1. clique no seu usuario no canto superior direito
2. acesse `Settings`
3. abra `Developer`
4. em `Access tokens`, clique em `Manage`
5. clique em `Generate new token`

#### Permissoes necessarias para o token

Tokens pessoais do Databricks usam escopos por token.
O ponto importante e que:

- Defina o escopos necessários para subir o projeto. Para fins de teste pode selecionar a opção "all api"

Depois disso, configure a autenticacao local:

```bash
databricks configure --token
```

Voce devera informar:

- `Databricks Host`: URL do seu workspace, por exemplo `https://dbc-xxxxxx.cloud.databricks.com`
- `Token`: o PAT gerado no passo anterior

### 3. Validar e publicar o bundle

```bash
databricks bundle validate
databricks bundle deploy -t dev
```

### 4. Executar o pipeline completo

```bash
databricks bundle run taxi_nyc_pipeline -t dev
```

O workflow executa:

1. download dos arquivos `yellow` e `green`
2. carga em `Landing`
3. processamento para `Bronze`
4. processamento para `Silver`
5. publicacao da `Gold`

## Plano B - Internet bloqueada na Free Edition

Se o download direto da TLC falhar no workspace, os arquivos podem ser baixados localmente e enviados para o volume da landing.

Os comandos abaixo estao em `bash`. No Windows, eles podem ser executados em um terminal como `Git Bash` ou `WSL`. No PowerShell, a sintaxe dos loops precisa ser adaptada.

### 1. Baixar localmente

```bash
for type in yellow green; do
  for m in 01 02 03 04 05; do
    curl -O "https://d37ci6vzurychx.cloudfront.net/trip-data/${type}_tripdata_2023-${m}.parquet"
  done
done
```

### 2. Criar pastas da landing

Se o step padrao de download falhar antes de criar os caminhos da landing, crie os diretorios manualmente antes do upload:

```bash
for type in yellow green; do
  for m in 01 02 03 04 05; do
    databricks fs mkdirs \
      "dbfs:/Volumes/ifood/taxi_nyc_landing/raw/${type}/year=2023/month=${m}"
  done
done
```

### 3. Enviar para o volume

```bash
for type in yellow green; do
  for m in 01 02 03 04 05; do
    databricks fs cp "${type}_tripdata_2023-${m}.parquet" \
      "dbfs:/Volumes/ifood/taxi_nyc_landing/raw/${type}/year=2023/month=${m}"
  done
done
```

### 4. Executar novamente o pipeline

Depois do upload, rode novamente o job completo:

```bash
databricks bundle run taxi_nyc_pipeline -t dev
```

Como os arquivos ja estarao presentes na landing, o step de download apenas fara `SKIP` para os arquivos existentes.

## Troubleshooting

### Erro temporario de import no Databricks

Se apos o deploy algum notebook falhar ao importar objetos de `src/common/` ou `src/pipeline/`, valide primeiro se o bundle foi publicado novamente:

```bash
databricks bundle deploy -t dev
```

Em seguida, rode o job novamente:

```bash
databricks bundle run taxi_nyc_pipeline -t dev
```

Em alguns casos no Databricks Free Edition/serverless, erros transitorios de import podem ser resolvidos com uma nova execucao do job ou reinicializacao da sessao.

### Download bloqueado no workspace

Se o workspace nao conseguir acessar a URL da TLC, use o Plano B acima para baixar localmente e enviar os arquivos para o volume.

### Landing inexistente no Plano B

Se o upload manual falhar por caminho inexistente, execute antes a etapa `Criar pastas da landing`.

## Onde Estao as Respostas do Case

Consultas finais:

- `analysis/02_case_answers.sql`

Material exploratorio:

- `analysis/01_eda.ipynb`

Tabelas finais para avaliacao:

- `ifood.taxi_nyc_gold.fact_trip`
- `ifood.taxi_nyc_gold.agg_trip_monthly_taxi`
- `ifood.taxi_nyc_gold.agg_trip_hourly_daily`
- `ifood.taxi_nyc_gold.dim_date`
- `ifood.taxi_nyc_gold.dim_vendor`

## Principais Decisoes Tecnicas

- `Landing -> Bronze -> Silver -> Gold` para separar recebimento, preservacao, conformidade e consumo
- notebooks finos + modulos Python reutilizaveis para equilibrar Databricks e testabilidade
- Auto Loader com `availableNow` para ingestao incremental orientada a arquivos
- tratamento ativo de schema drift na Bronze para evitar nulls artificiais e uso indevido de `_rescued_data`
- quarantine na Silver para proteger o contrato analitico da Gold
- `fact_trip` como base canonica e agregados materializados para facilitar consumo
- `MERGE` por `tripsk` para suportar processamento incremental e reruns mais seguros

## Observacoes

- O projeto foi pensado para Databricks Free Edition.
- A landing usa paths deterministicas por `year/month` para reduzir risco de duplicidade em reruns.
- A Gold foi modelada para responder o case de forma simples, mantendo uma fato detalhada e agregados prontos para consumo.
