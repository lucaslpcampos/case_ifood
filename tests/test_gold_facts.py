import pytest

from pipeline.processing.gold_facts import build_fact_union_parts, validate_fact_dimensions


class _FakeCatalog:
    def __init__(self, existing_tables):
        self._existing_tables = set(existing_tables)

    def tableExists(self, table_name):
        return table_name in self._existing_tables


class _FakeSparkForUnion:
    def __init__(self, existing_tables):
        self.catalog = _FakeCatalog(existing_tables)


class _FakeCountResult:
    def __init__(self, value):
        self._value = value

    def count(self):
        return self._value


class _FakeCollectResult:
    def __init__(self, value):
        self._value = value

    def collect(self):
        return [{"n": self._value}]


class _FakeSparkForValidation:
    def __init__(self, orphan_dates, orphan_vendors, rows_fact=10):
        self._orphan_dates = orphan_dates
        self._orphan_vendors = orphan_vendors
        self._rows_fact = rows_fact

    def table(self, table_name):
        return _FakeCountResult(self._rows_fact)

    def sql(self, query):
        if "LEFT ANTI JOIN" in query and "dim_date" in query:
            return _FakeCollectResult(self._orphan_dates)
        if "LEFT ANTI JOIN" in query and "dim_vendor" in query:
            return _FakeCollectResult(self._orphan_vendors)
        raise AssertionError(f"Unexpected query: {query}")


def test_build_fact_union_parts_fails_when_expected_silver_is_missing():
    spark = _FakeSparkForUnion({"ifood.taxi_nyc_silver.yellow_trips"})
    context = {
        "silver_inputs": {
            "yellow": "ifood.taxi_nyc_silver.yellow_trips",
            "green": "ifood.taxi_nyc_silver.green_trips",
        }
    }

    with pytest.raises(ValueError, match="Missing required silver tables"):
        build_fact_union_parts(spark, context)


def test_validate_fact_dimensions_fails_when_vendor_dimension_is_missing():
    spark = _FakeSparkForValidation(orphan_dates=0, orphan_vendors=2)
    context = {
        "fact_fqn": "ifood.taxi_nyc_gold.fact_trip",
        "dim_date_fqn": "ifood.taxi_nyc_gold.dim_date",
        "dim_vendor_fqn": "ifood.taxi_nyc_gold.dim_vendor",
    }

    with pytest.raises(
        AssertionError,
        match="rows in fact_trip are missing a matching dim_vendor row",
    ):
        validate_fact_dimensions(spark, context)
