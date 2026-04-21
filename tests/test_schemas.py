from common.schemas import (
    get_schema_cluster_by,
    get_schema_column_definitions,
    get_schema_column_names,
    get_schema_columns,
    render_ddl,
)


def test_render_ddl_uses_template_cluster_by():
    ddl = render_ddl(
        "yellow_bronze",
        catalog="ifood",
        schema="taxi_nyc_bronze",
        table="yellow_trips",
    )

    assert "CREATE TABLE IF NOT EXISTS ifood.taxi_nyc_bronze.yellow_trips" in ddl
    assert "tpep_pickup_datetime" in ddl
    assert "CLUSTER BY (tpep_pickup_datetime)" in ddl


def test_get_schema_column_names_preserves_declared_order():
    yellow_names = get_schema_column_names("yellow_bronze")

    assert yellow_names[:3] == [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]


def test_get_schema_columns_returns_name_and_dtype_pairs():
    silver_columns = get_schema_columns("trip_silver")

    assert ("VendorID", "INT") in silver_columns
    assert ("tripsk", "STRING") in silver_columns


def test_get_schema_column_definitions_exposes_nullable_flag():
    fact_columns = get_schema_column_definitions("fact_trip")
    tripsk = next(column for column in fact_columns if column["name"] == "tripsk")

    assert tripsk["nullable"] is False


def test_get_schema_cluster_by_returns_schema_cluster_columns():
    assert get_schema_cluster_by("agg_trip_monthly_taxi") == ("month_key",)


def test_render_ddl_applies_not_null_constraints():
    ddl = render_ddl(
        "trip_silver",
        catalog="ifood",
        schema="taxi_nyc_silver",
        table="yellow_trips",
    )

    assert "tripsk" in ddl
    assert "STRING NOT NULL" in ddl
