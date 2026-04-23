"""Builders SQL usados pelas tabelas analiticas da camada Gold."""


def build_dim_date_query(study_start_date: str, study_end_exclusive: str) -> str:
    """
    Gera a query SQL de construcao da dimensao de datas.

    Args:
        study_start_date: Data inicial inclusiva do intervalo de estudo.
        study_end_exclusive: Data final exclusiva do intervalo de estudo.

    Returns:
        Query SQL que expande uma linha por dia no intervalo informado.
    """
    return f"""
        SELECT
            INT(date_format(full_date, 'yyyyMMdd')) AS date_key,
            full_date,
            YEAR(full_date) AS year,
            QUARTER(full_date) AS quarter,
            MONTH(full_date) AS month,
            DAY(full_date) AS day,
            DAYOFWEEK(full_date) AS day_of_week,
            WEEKOFYEAR(full_date) AS week_of_year,
            CASE WHEN DAYOFWEEK(full_date) IN (1, 7) THEN true ELSE false END AS is_weekend
        FROM (
            SELECT explode(
                sequence(
                    to_date('{study_start_date}'),
                    date_sub(to_date('{study_end_exclusive}'), 1),
                    interval 1 day
                )
            ) AS full_date
        )
    """


def build_dim_vendor_query() -> str:
    """
    Gera a query SQL da dimensao estatica de vendors.

    Args:
        None.

    Returns:
        Query SQL com o mapeamento padrao de vendors da TLC.
    """
    return """
        SELECT CAST(vendor_id AS INT) AS vendor_id, vendor_name, vendor_short_name
        FROM VALUES
            (1, 'Creative Mobile Technologies, LLC', 'CMT'),
            (2, 'VeriFone Inc.', 'VeriFone'),
            (6, 'Myle Technologies Inc', 'Myle'),
            (7, 'Helix', 'Helix')
        AS t(vendor_id, vendor_name, vendor_short_name)
    """


def build_agg_trip_hourly_daily_query(fact_fqn: str) -> str:
    """
    Gera a query SQL do agregado diario por hora e tipo de taxi.

    Args:
        fact_fqn: Nome totalmente qualificado da fato transacional.

    Returns:
        Query SQL que agrega viagens por data, hora e tipo de taxi.
    """
    return f"""
        SELECT
            date_key,
            HOUR(pickup_datetime) AS pickup_hour,
            taxi_type,
            COUNT(*) AS trip_count,
            CAST(ROUND(SUM(total_amount), 2) AS DECIMAL(18,2)) AS total_amount_sum,
            CAST(SUM(passenger_count) AS BIGINT) AS passenger_count_sum,
            CAST(ROUND(AVG(total_amount), 2) AS DOUBLE) AS avg_total_amount,
            CAST(ROUND(AVG(passenger_count), 4) AS DOUBLE) AS avg_passenger_count
        FROM {fact_fqn}
        GROUP BY
            date_key,
            HOUR(pickup_datetime),
            taxi_type
    """


def build_agg_trip_monthly_taxi_query(fact_fqn: str) -> str:
    """
    Gera a query SQL do agregado mensal por tipo de taxi.

    Args:
        fact_fqn: Nome totalmente qualificado da fato transacional.

    Returns:
        Query SQL que agrega viagens por ano, mes e tipo de taxi.
    """
    return f"""
        SELECT
            INT(date_format(pickup_datetime, 'yyyyMM')) AS month_key,
            YEAR(pickup_datetime) AS year,
            MONTH(pickup_datetime) AS month,
            taxi_type,
            COUNT(*) AS trip_count,
            CAST(ROUND(SUM(total_amount), 2) AS DECIMAL(18,2)) AS total_amount_sum,
            CAST(SUM(passenger_count) AS BIGINT) AS passenger_count_sum,
            CAST(ROUND(AVG(total_amount), 2) AS DOUBLE) AS avg_total_amount,
            CAST(ROUND(AVG(passenger_count), 4) AS DOUBLE) AS avg_passenger_count
        FROM {fact_fqn}
        GROUP BY
            INT(date_format(pickup_datetime, 'yyyyMM')),
            YEAR(pickup_datetime),
            MONTH(pickup_datetime),
            taxi_type
    """


def build_fact_select_query(
    silver_fqn: str,
    taxi_type: str,
    study_start_date: str,
    study_end_exclusive: str,
) -> str:
    """
    Gera a query SQL que projeta uma tabela Silver para a fato canonica.

    Args:
        silver_fqn: Nome totalmente qualificado da tabela Silver de origem.
        taxi_type: Tipo de taxi associado a tabela Silver.
        study_start_date: Data inicial inclusiva do intervalo de estudo.
        study_end_exclusive: Data final exclusiva do intervalo de estudo.

    Returns:
        Query SQL pronta para compor a `fact_trip`.
    """
    return f"""
        SELECT
            s.tripsk,
            INT(date_format(s.pickup_datetime, 'yyyyMMdd')) AS date_key,
            s.VendorID AS vendor_id,
            s.passenger_count,
            s.total_amount,
            s.pickup_datetime,
            s.dropoff_datetime,
            BIGINT(unix_timestamp(s.dropoff_datetime) - unix_timestamp(s.pickup_datetime)) AS duration_seconds,
            '{taxi_type}' AS taxi_type,
            s._ingestion_ts
        FROM {silver_fqn} s
        WHERE s.pickup_datetime >= TIMESTAMP '{study_start_date}'
          AND s.pickup_datetime < TIMESTAMP '{study_end_exclusive}'
    """
