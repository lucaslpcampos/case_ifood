from common.config import SCHEMA_GOLD, STUDY_END_EXCLUSIVE, STUDY_START_DATE
from common.schemas import render_ddl
from common.sql import build_dim_date_query, build_dim_vendor_query
from common.utils import ensure_schemas


def build_gold_dimension_context(catalog):
    gold_schema = f"{catalog}.{SCHEMA_GOLD}"
    return {
        "catalog": catalog,
        "gold_schema": gold_schema,
        "dim_date_fqn": f"{gold_schema}.dim_date",
        "dim_vendor_fqn": f"{gold_schema}.dim_vendor",
    }


def ensure_gold_dimension_tables(spark, context):
    ensure_schemas(spark, context["catalog"], [SCHEMA_GOLD])
    spark.sql(render_ddl("dim_date", catalog=context["catalog"], schema=SCHEMA_GOLD))
    spark.sql(render_ddl("dim_vendor", catalog=context["catalog"], schema=SCHEMA_GOLD))


def build_dim_date_df(spark):
    return spark.sql(build_dim_date_query(STUDY_START_DATE, STUDY_END_EXCLUSIVE))


def build_dim_vendor_df(spark):
    return spark.sql(build_dim_vendor_query())


def write_delta_table(df, target_fqn):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_fqn)
    )


def validate_dim_date(spark, dim_date_fqn):
    rows_date = spark.table(dim_date_fqn).count()
    assert rows_date == 151, f"dim_date should have 151 rows, found {rows_date}"
    return rows_date


def summarize_dimensions(spark, context):
    rows_date = spark.table(context["dim_date_fqn"]).count()
    rows_vendor = spark.table(context["dim_vendor_fqn"]).count()
    print(f"dim_date: {rows_date} rows")
    print(f"dim_vendor: {rows_vendor} rows")
    return {
        "dim_date_rows": rows_date,
        "dim_vendor_rows": rows_vendor,
        "dim_date_fqn": context["dim_date_fqn"],
        "dim_vendor_fqn": context["dim_vendor_fqn"],
    }
