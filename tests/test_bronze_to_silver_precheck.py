import pytest
from pyspark.sql import Row

from pipeline.processing.bronze_to_silver import validate_merge_source_uniqueness


def test_validate_merge_source_uniqueness_accepts_unique_source(spark):
    df = spark.createDataFrame(
        [
            Row(tripsk="a", VendorID=1),
            Row(tripsk="b", VendorID=2),
        ]
    )

    result = validate_merge_source_uniqueness(df)
    assert result["duplicate_key_count"] == 0
    assert result["sample"] == []


def test_validate_merge_source_uniqueness_raises_helpful_error(spark):
    df = spark.createDataFrame(
        [
            Row(tripsk="dup", VendorID=1),
            Row(tripsk="dup", VendorID=2),
            Row(tripsk="ok", VendorID=3),
        ]
    )

    with pytest.raises(ValueError, match="duplicated tripsk values"):
        validate_merge_source_uniqueness(df)
