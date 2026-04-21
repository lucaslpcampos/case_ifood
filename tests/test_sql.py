from common.sql import (
    build_agg_trip_hourly_daily_query,
    build_agg_trip_monthly_taxi_query,
    build_dim_date_query,
    build_dim_vendor_query,
    build_fact_select_query,
)


def test_build_dim_date_query_uses_runtime_study_range():
    query = build_dim_date_query("2023-01-01", "2023-06-01")

    assert "to_date('2023-01-01')" in query
    assert "date_sub(to_date('2023-06-01'), 1)" in query


def test_build_dim_vendor_query_contains_expected_vendor_dimension_rows():
    query = build_dim_vendor_query()

    assert "Creative Mobile Technologies, LLC" in query
    assert "VeriFone Inc." in query


def test_build_fact_select_query_filters_study_range_and_sets_taxi_type():
    query = build_fact_select_query(
        silver_fqn="ifood.taxi_nyc_silver.yellow_trips",
        taxi_type="yellow",
        study_start_date="2023-01-01",
        study_end_exclusive="2023-06-01",
    )

    assert "FROM ifood.taxi_nyc_silver.yellow_trips s" in query
    assert "'yellow' AS taxi_type" in query
    assert "TIMESTAMP '2023-01-01'" in query
    assert "TIMESTAMP '2023-06-01'" in query


def test_build_aggregate_queries_use_fact_table_reference():
    hourly_query = build_agg_trip_hourly_daily_query("ifood.taxi_nyc_gold.fact_trip")
    monthly_query = build_agg_trip_monthly_taxi_query("ifood.taxi_nyc_gold.fact_trip")

    assert "FROM ifood.taxi_nyc_gold.fact_trip" in hourly_query
    assert "GROUP BY" in hourly_query
    assert "FROM ifood.taxi_nyc_gold.fact_trip" in monthly_query
    assert "month_key" in monthly_query
