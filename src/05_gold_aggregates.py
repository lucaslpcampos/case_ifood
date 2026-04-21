# Databricks notebook source

dbutils.widgets.text("catalog", "ifood")

from pipeline.processing.gold_aggregates import (
    build_agg_hourly_daily_df,
    build_agg_monthly_taxi_df,
    build_gold_aggregate_context,
    ensure_gold_aggregate_tables,
    summarize_aggregates,
    write_agg_hourly_daily,
    write_agg_monthly_taxi,
)

catalog = dbutils.widgets.get("catalog")
context = build_gold_aggregate_context(catalog)

print(f"fact_trip={context['fact_fqn']}")
print(
    f"agg_trip_hourly_daily={context['agg_hourly_daily_fqn']}  "
    f"agg_trip_monthly_taxi={context['agg_monthly_taxi_fqn']}"
)

ensure_gold_aggregate_tables(spark, context)
agg_hourly_daily_df = build_agg_hourly_daily_df(spark, context["fact_fqn"])
write_agg_hourly_daily(agg_hourly_daily_df, context["agg_hourly_daily_fqn"])
agg_monthly_taxi_df = build_agg_monthly_taxi_df(spark, context["fact_fqn"])
write_agg_monthly_taxi(agg_monthly_taxi_df, context["agg_monthly_taxi_fqn"])
metrics = summarize_aggregates(spark, context)

dbutils.notebook.exit(
    "OK "
    f"hourly_daily_rows={metrics['hourly_daily_rows']} "
    f"monthly_taxi_rows={metrics['monthly_taxi_rows']}"
)
