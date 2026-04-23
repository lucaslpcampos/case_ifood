"""Funcoes de download e planejamento da landing zone."""

import os
import time

import requests
from pyspark.sql import SparkSession

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


def download_file(url: str, target: str, timeout: int, max_retries: int) -> int:
    """
    Faz o download de um arquivo para o destino final com retry e arquivo temporario.

    Args:
        url: URL de origem do arquivo.
        target: Path final onde o arquivo deve ser salvo.
        timeout: Timeout de cada tentativa de requisicao, em segundos.
        max_retries: Numero maximo de tentativas de download.

    Returns:
        Quantidade de bytes gravados no arquivo final.
    """
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


def build_download_context(taxi_type: str, catalog: str) -> dict[str, object]:
    """
    Monta o contexto operacional usado pelo step de download.

    Args:
        taxi_type: Tipo de taxi configurado em `TAXI_CONFIGS`.
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        Dicionario com configuracoes e paths necessarios para o download.
    """
    taxi_cfg = TAXI_CONFIGS[taxi_type]
    landing_path = get_landing_path(catalog, taxi_type)
    return {
        "taxi_type": taxi_type,
        "catalog": catalog,
        "taxi_cfg": taxi_cfg,
        "landing_path": landing_path,
    }


def ensure_landing_storage(spark: SparkSession, catalog: str) -> None:
    """
    Garante a existencia do schema e do volume usados pela landing zone.

    Args:
        spark: Sessao Spark ativa.
        catalog: Nome do catalogo Unity Catalog.

    Returns:
        None. A funcao executa DDLs para preparar a landing.
    """
    ensure_schemas(spark, catalog, [SCHEMA_LANDING])
    ensure_volume(spark, catalog, SCHEMA_LANDING, VOLUME_NAME)


def build_download_plan(context: dict[str, object]) -> list[dict[str, object]]:
    """
    Gera o plano de download dos arquivos esperados para um tipo de taxi.

    Args:
        context: Contexto operacional retornado por `build_download_context`.

    Returns:
        Lista ordenada de itens com URL, particao e destino de cada arquivo.
    """
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


def execute_download_plan(plan: list[dict[str, object]]) -> dict[str, int]:
    """
    Executa o plano de download e contabiliza arquivos baixados e ignorados.

    Args:
        plan: Lista de itens retornada por `build_download_plan`.

    Returns:
        Dicionario com a quantidade de arquivos baixados e pulados.
    """
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
