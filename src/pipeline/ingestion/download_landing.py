import os
import time
from datetime import date

import requests

from common.config import (
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_TIMEOUT_SECONDS,
    MONTHS,
    SCHEMA_LANDING,
    TAXI_CONFIGS,
    VOLUME_NAME,
    get_landing_path,
)
from common.utils import ensure_schemas, ensure_volume


def download_file(url, target, timeout, max_retries):
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                os.makedirs(os.path.dirname(target), exist_ok=True)
                bytes_written = 0
                with open(target, "wb") as file_handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            file_handle.write(chunk)
                            bytes_written += len(chunk)
                return bytes_written
        except (requests.RequestException, OSError) as exc:
            last_exc = exc
            print(f"  attempt {attempt} failed: {exc}")
            time.sleep(2 * attempt)

    raise RuntimeError(
        f"download failed after {max_retries} attempts: {url}"
    ) from last_exc


def build_download_context(taxi_type, catalog):
    taxi_cfg = TAXI_CONFIGS[taxi_type]
    landing_path = get_landing_path(catalog, taxi_type)
    ingestion_date = date.today().isoformat()
    return {
        "taxi_type": taxi_type,
        "catalog": catalog,
        "taxi_cfg": taxi_cfg,
        "landing_path": landing_path,
        "ingestion_date": ingestion_date,
    }


def ensure_landing_storage(spark, catalog):
    ensure_schemas(spark, catalog, [SCHEMA_LANDING])
    ensure_volume(spark, catalog, SCHEMA_LANDING, VOLUME_NAME)


def build_download_plan(context):
    taxi_type = context["taxi_type"]
    taxi_cfg = context["taxi_cfg"]
    landing_path = context["landing_path"]
    ingestion_date = context["ingestion_date"]

    plan = []
    for year, month in MONTHS:
        url = taxi_cfg["source_url_template"].format(year=year, month=month)
        partition = f"dt_ingestion={ingestion_date}"
        filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        target = f"{landing_path}/{partition}/{filename}"
        plan.append(
            {
                "year": year,
                "month": month,
                "url": url,
                "partition": partition,
                "filename": filename,
                "target": target,
            }
        )
    return plan


def execute_download_plan(plan):
    downloaded_files = 0
    skipped_files = 0

    for item in plan:
        if os.path.exists(item["target"]):
            print(f"SKIP (already exists): {item['target']}")
            skipped_files += 1
            continue

        size = download_file(
            item["url"],
            item["target"],
            DOWNLOAD_TIMEOUT_SECONDS,
            DOWNLOAD_MAX_RETRIES,
        )
        downloaded_files += 1
        print(f"OK: {item['filename']} ({size / 1024 / 1024:.1f} MB)")

    return {
        "downloaded_files": downloaded_files,
        "skipped_files": skipped_files,
    }
