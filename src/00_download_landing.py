# Databricks notebook source

dbutils.widgets.text("taxi_type", "yellow")
dbutils.widgets.text("catalog", "ifood")

from pipeline.ingestion.download_landing import (
    build_download_context,
    build_download_plan,
    ensure_landing_storage,
    execute_download_plan,
)

taxi_type = dbutils.widgets.get("taxi_type")
catalog = dbutils.widgets.get("catalog")
context = build_download_context(taxi_type, catalog)

print(
    f"taxi_type={context['taxi_type']}  "
    f"catalog={context['catalog']}  "
    f"landing_path={context['landing_path']}"
)

ensure_landing_storage(spark, catalog)
plan = build_download_plan(context)
metrics = execute_download_plan(plan)

dbutils.notebook.exit(
    "OK "
    f"downloaded={metrics['downloaded_files']} "
    f"skipped={metrics['skipped_files']}"
)
