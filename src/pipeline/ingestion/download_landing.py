import os
import time

import requests

from common.config import (
    DOWNLOAD_MAX_RETRIES,
    DOWNLOAD_TIMEOUT_SECONDS,
    MONTHS,
    SCHEMA_LANDING,
    TAXI_CONFIGS,
    VOLUME_NAME,
    get_landing_month_path,
    get_landing_path,
)
from common.utils import ensure_schemas, ensure_volume


def download_file(url, target, timeout, max_retries):
    last_exc = None
    temp_target = f"{target}.part"
    for attempt in range(1, max_retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                os.makedirs(os.path.dirname(target), exist_ok=True)
                if os.path.exists(temp_target):
                    os.remove(temp_target)
                bytes_written = 0
                with open(temp_target, "wb") as file_handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            file_handle.write(chunk)
                            bytes_written += len(chunk)
                os.replace(temp_target, target)
                return bytes_written
        except (requests.RequestException, OSError) as exc:
            last_exc = exc
            if os.path.exists(temp_target):
                os.remove(temp_target)
            print(f"  attempt {attempt} failed: {exc}")
            time.sleep(2 * attempt)

    raise RuntimeError(
        f"download failed after {max_retries} attempts: {url}"
    ) from last_exc


def build_download_context(taxi_type, catalog):
    taxi_cfg = TAXI_CONFIGS[taxi_type]
    landing_path = get_landing_path(catalog, taxi_type)
    return {
        "taxi_type": taxi_type,
        "catalog": catalog,
        "taxi_cfg": taxi_cfg,
        "landing_path": landing_path,
    }


def ensure_landing_storage(spark, catalog):
    ensure_schemas(spark, catalog, [SCHEMA_LANDING])
    ensure_volume(spark, catalog, SCHEMA_LANDING, VOLUME_NAME)


def build_download_plan(context):
    taxi_type = context["taxi_type"]
    taxi_cfg = context["taxi_cfg"]

    plan = []
    for year, month in MONTHS:
        url = taxi_cfg["source_url_template"].format(year=year, month=month)
        partition = f"year={year}/month={month:02d}"
        filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        target = f"{get_landing_month_path(context['catalog'], taxi_type, year, month)}/{filename}"
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
