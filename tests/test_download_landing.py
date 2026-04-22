import pytest

from pipeline.ingestion.download_landing import (
    build_download_context,
    build_download_plan,
    download_file,
)


def test_build_download_plan_uses_year_month_source_partition():
    context = build_download_context("green", "ifood")

    plan = build_download_plan(context)

    assert plan[0]["partition"] == "year=2023/month=01"
    assert plan[0]["target"].endswith(
        "/raw/green/year=2023/month=01/green_tripdata_2023-01.parquet"
    )
    assert plan[-1]["partition"] == "year=2023/month=05"
    assert plan[-1]["target"].endswith(
        "/raw/green/year=2023/month=05/green_tripdata_2023-05.parquet"
    )


def test_download_file_removes_partial_target_when_download_fails(tmp_path, monkeypatch):
    target = tmp_path / "green_tripdata_2023-03.parquet"

    class FailingResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            yield b"partial-parquet-bytes"
            raise OSError("connection dropped")

    monkeypatch.setattr(
        "pipeline.ingestion.download_landing.requests.get",
        lambda *args, **kwargs: FailingResponse(),
    )
    monkeypatch.setattr(
        "pipeline.ingestion.download_landing.time.sleep",
        lambda seconds: None,
    )

    with pytest.raises(RuntimeError):
        download_file(
            "https://example.com/green_tripdata_2023-03.parquet",
            str(target),
            timeout=1,
            max_retries=1,
        )

    assert not target.exists()
