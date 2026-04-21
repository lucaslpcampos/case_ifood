# Databricks notebook source

dbutils.widgets.text("taxi_type", "yellow")
dbutils.widgets.text("catalog", "ifood")

from pipeline.processing.bronze_to_silver import (
    append_quarantine_df,
    build_silver_context,
    build_silver_source_df,
    ensure_silver_tables,
    validate_merge_source_uniqueness,
    merge_silver_df,
    project_silver_df,
    split_silver_clean_and_quarantine,
    summarize_silver_quality,
    summarize_silver_source,
    validate_silver,
)

taxi_type = dbutils.widgets.get("taxi_type")
catalog = dbutils.widgets.get("catalog")
context = build_silver_context(taxi_type, catalog)

print(
    f"taxi_type={context['taxi_type']}  "
    f"bronze={context['bronze_fqn']}  "
    f"silver={context['silver_fqn']}"
)

ensure_silver_tables(spark, context)
source_df = build_silver_source_df(spark, context)
rows_in = summarize_silver_source(source_df)
clean_df, quarantine_df = split_silver_clean_and_quarantine(source_df)
quality_metrics = summarize_silver_quality(rows_in, clean_df, quarantine_df)
silver_df = project_silver_df(clean_df)
validate_merge_source_uniqueness(silver_df)
merge_silver_df(spark, context["silver_fqn"], silver_df)
append_quarantine_df(context["quarantine_fqn"], quarantine_df)
validation_metrics = validate_silver(spark, context["silver_fqn"])

dbutils.notebook.exit(
    "OK "
    f"rows={validation_metrics['rows']} "
    f"quarantine={quality_metrics['rows_quarantine']}"
)
