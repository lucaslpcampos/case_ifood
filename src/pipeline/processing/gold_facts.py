from delta.tables import DeltaTable

from common.config import (
    FACT_SOURCE_TAXI_TYPES,
    SCHEMA_GOLD,
    SCHEMA_SILVER,
    STUDY_END_EXCLUSIVE,
    STUDY_START_DATE,
    TAXI_CONFIGS,
    get_fqn,
)
from common.quality import apply_gold_constraints
from common.schemas import render_ddl
from common.sql import build_fact_select_query
from common.utils import ensure_schemas


def build_gold_fact_context(catalog):
    gold_schema = f"{catalog}.{SCHEMA_GOLD}"
    silver_inputs = {
        taxi_type: get_fqn(catalog, SCHEMA_SILVER, TAXI_CONFIGS[taxi_type]["silver_table"])
        for taxi_type in FACT_SOURCE_TAXI_TYPES
    }
    return {
        "catalog": catalog,
        "gold_schema": gold_schema,
        "fact_fqn": f"{gold_schema}.fact_trip",
        "dim_date_fqn": f"{gold_schema}.dim_date",
        "dim_vendor_fqn": f"{gold_schema}.dim_vendor",
        "silver_inputs": silver_inputs,
    }


def ensure_gold_fact_table(spark, context):
    ensure_schemas(spark, context["catalog"], [SCHEMA_GOLD])
    spark.sql(render_ddl("fact_trip", catalog=context["catalog"], schema=SCHEMA_GOLD))
    apply_gold_constraints(spark, context["fact_fqn"], "fact_trip")


def build_fact_union_parts(spark, context):
    missing_tables = [
        silver_fqn
        for silver_fqn in context["silver_inputs"].values()
        if not spark.catalog.tableExists(silver_fqn)
    ]
    if missing_tables:
        missing_list = ", ".join(sorted(missing_tables))
        raise ValueError(f"Missing required silver tables: {missing_list}")

    union_parts = []
    for taxi_type, silver_fqn in context["silver_inputs"].items():
        union_parts.append(
            build_fact_select_query(
                silver_fqn,
                taxi_type,
                STUDY_START_DATE,
                STUDY_END_EXCLUSIVE,
            )
        )
    return union_parts


def build_fact_df(spark, union_parts):
    assert union_parts, "No silver tables available"
    return spark.sql(" UNION ALL ".join(union_parts))


def summarize_fact_source(fact_df):
    rows_for_merge = fact_df.count()
    print(f"Rows for MERGE: {rows_for_merge:,}")
    return rows_for_merge


def merge_fact_df(spark, fact_fqn, fact_df):
    target_table = DeltaTable.forName(spark, fact_fqn)
    (
        target_table.alias("target")
        .merge(fact_df.alias("source"), "target.tripsk = source.tripsk")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def validate_fact_dimensions(spark, context):
    rows_fact = spark.table(context["fact_fqn"]).count()
    orphan_dates = spark.sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {context['fact_fqn']} f
        LEFT ANTI JOIN {context['dim_date_fqn']} d ON f.date_key = d.date_key
        """
    ).collect()[0]["n"]
    orphan_vendors = spark.sql(
        f"""
        SELECT COUNT(*) AS n
        FROM {context['fact_fqn']} f
        LEFT ANTI JOIN {context['dim_vendor_fqn']} v ON f.vendor_id = v.vendor_id
        """
    ).collect()[0]["n"]

    assert orphan_dates == 0, (
        f"{orphan_dates} rows in fact_trip are missing a matching dim_date row"
    )
    assert orphan_vendors == 0, (
        f"{orphan_vendors} rows in fact_trip are missing a matching dim_vendor row"
    )
    return {
        "rows": rows_fact,
        "orphan_dates": orphan_dates,
        "orphan_vendors": orphan_vendors,
    }


def summarize_fact(metrics):
    print(f"fact_trip: {metrics['rows']:,} rows")
    print(
        f"Orphans dim_date: {metrics['orphan_dates']}  "
        f"dim_vendor: {metrics['orphan_vendors']}"
    )
