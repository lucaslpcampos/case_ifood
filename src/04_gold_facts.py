# Databricks notebook source

dbutils.widgets.text("catalog", "ifood")

from pipeline.processing.gold_facts import (
    build_fact_df,
    build_fact_union_parts,
    build_gold_fact_context,
    ensure_gold_fact_table,
    merge_fact_df,
    summarize_fact,
    summarize_fact_source,
    validate_fact_dimensions,
)

catalog = dbutils.widgets.get("catalog")
context = build_gold_fact_context(catalog)
silver_inputs_text = "  ".join(
    f"{taxi_type}={silver_fqn}"
    for taxi_type, silver_fqn in context["silver_inputs"].items()
)

print(
    f"fact_trip={context['fact_fqn']}  "
    f"{silver_inputs_text}"
)

ensure_gold_fact_table(spark, context)
union_parts = build_fact_union_parts(spark, context)
fact_df = build_fact_df(spark, union_parts)
summarize_fact_source(fact_df)
merge_fact_df(spark, context["fact_fqn"], fact_df)
metrics = validate_fact_dimensions(spark, context)
summarize_fact(metrics)

dbutils.notebook.exit(
    "OK "
    f"rows={metrics['rows']} "
    f"orphan_dates={metrics['orphan_dates']} "
    f"orphan_vendors={metrics['orphan_vendors']}"
)
