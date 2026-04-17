# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 — Download Source Files → Landing (UC Volume)
# MAGIC Baixa os parquets da TLC (CloudFront) para o UC Volume de landing.
# MAGIC
# MAGIC **Genérico:** recebe `taxi_type` (yellow/green) via widget.
# MAGIC
# MAGIC Se o download falhar (internet bloqueada em Free Edition), sai com **exit 2**
# MAGIC e o README documenta o Plano B (upload manual via `databricks fs cp`).

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/utils

# COMMAND ----------

import os
import time
from datetime import date

import requests

# COMMAND ----------

dbutils.widgets.text("taxi_type", "yellow")
dbutils.widgets.text("catalog", "ifood")

taxi_type = dbutils.widgets.get("taxi_type")
catalog = dbutils.widgets.get("catalog")

taxi_cfg = TAXI_CONFIGS[taxi_type]
landing_path = get_landing_path(catalog, taxi_type)
ingestion_date = date.today().isoformat()

print(f"taxi_type={taxi_type}  catalog={catalog}  landing_path={landing_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — criar schemas e volume

# COMMAND ----------

ensure_schemas(spark, catalog, [SCHEMA_LANDING])
ensure_volume(spark, catalog, SCHEMA_LANDING, VOLUME_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download

# COMMAND ----------

def download_file(url, target, timeout, max_retries):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as resp:
                resp.raise_for_status()
                os.makedirs(os.path.dirname(target), exist_ok=True)
                bytes_written = 0
                with open(target, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            fh.write(chunk)
                            bytes_written += len(chunk)
                return bytes_written
        except (requests.RequestException, OSError) as exc:
            last_exc = exc
            print(f"  tentativa {attempt} falhou: {exc}")
            time.sleep(2 * attempt)
    raise RuntimeError(f"download falhou apos {max_retries} tentativas: {url}") from last_exc

# COMMAND ----------

for year, month in MONTHS:
    url = taxi_cfg["source_url_template"].format(year=year, month=month)
    partition = f"dt_ingestion={ingestion_date}"
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    target = f"{landing_path}/{partition}/{filename}"

    if os.path.exists(target):
        print(f"SKIP (já existe): {target}")
        continue

    try:
        size = download_file(url, target, DOWNLOAD_TIMEOUT_SECONDS, DOWNLOAD_MAX_RETRIES)
        print(f"OK: {filename} ({size / 1024 / 1024:.1f} MB)")
    except Exception as e:
        print(f"ERRO: {e}")
        print("Consulte o README para o Plano B (upload manual).")
        dbutils.notebook.exit("DOWNLOAD_FAILED")

# COMMAND ----------

print(f"Download completo. Arquivos em: {landing_path}")
dbutils.notebook.exit("OK")
