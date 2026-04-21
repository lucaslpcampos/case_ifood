# Databricks notebook source

dbutils.widgets.text("taxi_type", "yellow")
dbutils.widgets.text("catalog", "ifood")

from common.utils import new_batch_id
from pipeline.ingestion.landing_to_bronze import (
    build_bronze_context,
    build_bronze_stream_df,
    build_raw_stream_df,
    ensure_bronze_table,
    enrich_stream_df,
    summarize_bronze,
    write_bronze_stream,
)

taxi_type = dbutils.widgets.get("taxi_type")
catalog = dbutils.widgets.get("catalog")
context = build_bronze_context(taxi_type, catalog)

print(
    f"taxi_type={context['taxi_type']}  "
    f"landing={context['landing_path']}  "
    f"bronze={context['bronze_fqn']}"
)

ensure_bronze_table(spark, context)
batch_id = new_batch_id()
raw_stream_df = build_raw_stream_df(spark, context)
enriched_stream_df = enrich_stream_df(raw_stream_df, batch_id)
stream_df = build_bronze_stream_df(enriched_stream_df, context)
write_bronze_stream(stream_df, context)
result = summarize_bronze(spark, context, batch_id)

dbutils.notebook.exit(
    "OK "
    f"rows={result['rows']} "
    f"batch_id={result['batch_id']} "
    f"rescued_rows={result['rescued_rows']}"
)
