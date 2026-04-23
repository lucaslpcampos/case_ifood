from common.schemas import (
    apply_table_comments,
    get_schema_cluster_by,
    get_schema_column_comments,
    get_table_comment,
    render_comment_ddls,
    render_ddl,
)


class _FakeSpark:
    def __init__(self):
        self.executed_sql = []

    def sql(self, statement):
        self.executed_sql.append(statement)


def test_schema_comment_helpers_expose_silver_and_gold_metadata():
    assert get_table_comment("trip_silver") is not None
    assert get_table_comment("fact_trip") is not None

    silver_comments = get_schema_column_comments("trip_silver")
    fact_comments = get_schema_column_comments("fact_trip")

    assert "tripsk" in silver_comments
    assert "pickup_datetime" in silver_comments
    assert "taxi_type" in fact_comments


def test_apply_table_comments_executes_table_and_column_comment_ddls():
    spark = _FakeSpark()

    ddls = render_comment_ddls(
        "fact_trip",
        catalog="ifood",
        schema="taxi_nyc_gold",
    )
    executed = apply_table_comments(
        spark,
        "fact_trip",
        catalog="ifood",
        schema="taxi_nyc_gold",
    )

    assert ddls
    assert executed == ddls
    assert spark.executed_sql == ddls
    assert any("COMMENT ON TABLE ifood.taxi_nyc_gold.fact_trip" in ddl for ddl in ddls)
    assert any("ALTER TABLE ifood.taxi_nyc_gold.fact_trip ALTER COLUMN tripsk COMMENT" in ddl for ddl in ddls)


def test_physical_layout_clusters_only_selected_silver_and_gold_fact_tables():
    assert get_schema_cluster_by("yellow_bronze") == ()
    assert get_schema_cluster_by("green_bronze") == ()
    assert get_schema_cluster_by("trip_silver") == ("pickup_datetime",)
    assert get_schema_cluster_by("fact_trip") == ("taxi_type", "pickup_datetime")
    assert get_schema_cluster_by("agg_trip_monthly_taxi") == ()

    fact_ddl = render_ddl("fact_trip", catalog="ifood", schema="taxi_nyc_gold")
    bronze_ddl = render_ddl("yellow_bronze", catalog="ifood", schema="taxi_nyc_bronze")

    assert "CLUSTER BY (taxi_type, pickup_datetime)" in fact_ddl
    assert "CLUSTER BY" not in bronze_ddl
