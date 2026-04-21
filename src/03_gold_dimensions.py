# Databricks notebook source

dbutils.widgets.text("catalog", "ifood")

from pipeline.processing.gold_dimensions import (
    build_dim_date_df,
    build_dim_vendor_df,
    build_gold_dimension_context,
    ensure_gold_dimension_tables,
    summarize_dimensions,
    validate_dim_date,
    write_delta_table,
)

catalog = dbutils.widgets.get("catalog")
context = build_gold_dimension_context(catalog)

print(f"catalog={catalog}  gold_schema={context['gold_schema']}")

ensure_gold_dimension_tables(spark, context)
dim_date_df = build_dim_date_df(spark)
write_delta_table(dim_date_df, context["dim_date_fqn"])
validate_dim_date(spark, context["dim_date_fqn"])
dim_vendor_df = build_dim_vendor_df(spark)
write_delta_table(dim_vendor_df, context["dim_vendor_fqn"])
result = summarize_dimensions(spark, context)

dbutils.notebook.exit(
    "OK "
    f"dim_date={result['dim_date_rows']} "
    f"dim_vendor={result['dim_vendor_rows']}"
)
