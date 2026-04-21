from common.config import SCHEMA_GOLD
from common.schemas import render_ddl
from common.sql import (
    build_agg_trip_hourly_daily_query,
    build_agg_trip_monthly_taxi_query,
)
from common.utils import ensure_schemas


def build_gold_aggregate_context(catalog):
    gold_schema = f"{catalog}.{SCHEMA_GOLD}"
    return {
        "catalog": catalog,
        "gold_schema": gold_schema,
        "fact_fqn": f"{gold_schema}.fact_trip",
        "agg_hourly_daily_fqn": f"{gold_schema}.agg_trip_hourly_daily",
        "agg_monthly_taxi_fqn": f"{gold_schema}.agg_trip_monthly_taxi",
    }


def ensure_gold_aggregate_tables(spark, context):
    ensure_schemas(spark, context["catalog"], [SCHEMA_GOLD])
    spark.sql(
        render_ddl("agg_trip_hourly_daily", catalog=context["catalog"], schema=SCHEMA_GOLD)
    )
    spark.sql(
        render_ddl("agg_trip_monthly_taxi", catalog=context["catalog"], schema=SCHEMA_GOLD)
    )


def build_agg_hourly_daily_df(spark, fact_fqn):
    return spark.sql(build_agg_trip_hourly_daily_query(fact_fqn))


def write_overwrite_table(df, source_view_name, target_fqn):
    spark = df.sparkSession
    df.createOrReplaceTempView(source_view_name)
    spark.sql(
        f"""
        INSERT OVERWRITE TABLE {target_fqn}
        SELECT *
        FROM {source_view_name}
        """
    )


def write_agg_hourly_daily(agg_hourly_daily_df, agg_hourly_daily_fqn):
    write_overwrite_table(
        agg_hourly_daily_df,
        "agg_trip_hourly_daily_source",
        agg_hourly_daily_fqn,
    )


def build_agg_monthly_taxi_df(spark, fact_fqn):
    return spark.sql(build_agg_trip_monthly_taxi_query(fact_fqn))


def write_agg_monthly_taxi(agg_monthly_taxi_df, agg_monthly_taxi_fqn):
    write_overwrite_table(
        agg_monthly_taxi_df,
        "agg_trip_monthly_taxi_source",
        agg_monthly_taxi_fqn,
    )


def summarize_aggregates(spark, context):
    hourly_daily_rows = spark.table(context["agg_hourly_daily_fqn"]).count()
    monthly_taxi_rows = spark.table(context["agg_monthly_taxi_fqn"]).count()
    print(f"agg_trip_hourly_daily: {hourly_daily_rows:,} rows")
    print(f"agg_trip_monthly_taxi: {monthly_taxi_rows:,} rows")
    return {
        "hourly_daily_rows": hourly_daily_rows,
        "monthly_taxi_rows": monthly_taxi_rows,
    }
