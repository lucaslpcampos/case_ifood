from common.config import (
    FACT_SOURCE_TAXI_TYPES,
    TAXI_CONFIGS,
    get_landing_month_path,
    get_landing_path,
)


def test_taxi_configs_expose_runtime_settings_for_both_taxi_types():
    assert set(TAXI_CONFIGS) == {"yellow", "green"}
    assert TAXI_CONFIGS["yellow"]["pickup_col"] == "tpep_pickup_datetime"
    assert TAXI_CONFIGS["green"]["pickup_col"] == "lpep_pickup_datetime"
    assert FACT_SOURCE_TAXI_TYPES == ("yellow", "green")


def test_get_landing_path_uses_taxi_landing_subpath():
    assert get_landing_path("ifood", "yellow").endswith("/raw/yellow")
    assert get_landing_path("ifood", "green").endswith("/raw/green")


def test_get_landing_month_path_uses_deterministic_source_partition():
    assert get_landing_month_path("ifood", "yellow", 2023, 1).endswith(
        "/raw/yellow/year=2023/month=01"
    )
    assert get_landing_month_path("ifood", "green", 2023, 5).endswith(
        "/raw/green/year=2023/month=05"
    )
