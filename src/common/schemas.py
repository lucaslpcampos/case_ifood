def _column(name, dtype, nullable=True):
    return {
        "name": name,
        "dtype": dtype,
        "nullable": nullable,
    }


TABLE_SCHEMAS = {
    "yellow_bronze": {
        "table_name": "yellow_trips",
        "columns": [
            _column("VendorID", "BIGINT"),
            _column("tpep_pickup_datetime", "TIMESTAMP"),
            _column("tpep_dropoff_datetime", "TIMESTAMP"),
            _column("passenger_count", "DOUBLE"),
            _column("trip_distance", "DOUBLE"),
            _column("RatecodeID", "DOUBLE"),
            _column("store_and_fwd_flag", "STRING"),
            _column("PULocationID", "BIGINT"),
            _column("DOLocationID", "BIGINT"),
            _column("payment_type", "DOUBLE"),
            _column("fare_amount", "DOUBLE"),
            _column("extra", "DOUBLE"),
            _column("mta_tax", "DOUBLE"),
            _column("tip_amount", "DOUBLE"),
            _column("tolls_amount", "DOUBLE"),
            _column("improvement_surcharge", "DOUBLE"),
            _column("total_amount", "DOUBLE"),
            _column("congestion_surcharge", "DOUBLE"),
            _column("airport_fee", "DOUBLE"),
            _column("_ingestion_ts", "TIMESTAMP"),
            _column("_source_file", "STRING"),
            _column("_batch_id", "STRING"),
            _column("_rescued_data", "STRING"),
        ],
        "cluster_by": ("tpep_pickup_datetime",),
    },
    "green_bronze": {
        "table_name": "green_trips",
        "columns": [
            _column("VendorID", "BIGINT"),
            _column("lpep_pickup_datetime", "TIMESTAMP"),
            _column("lpep_dropoff_datetime", "TIMESTAMP"),
            _column("passenger_count", "DOUBLE"),
            _column("trip_distance", "DOUBLE"),
            _column("RatecodeID", "DOUBLE"),
            _column("store_and_fwd_flag", "STRING"),
            _column("PULocationID", "BIGINT"),
            _column("DOLocationID", "BIGINT"),
            _column("payment_type", "DOUBLE"),
            _column("fare_amount", "DOUBLE"),
            _column("extra", "DOUBLE"),
            _column("mta_tax", "DOUBLE"),
            _column("tip_amount", "DOUBLE"),
            _column("tolls_amount", "DOUBLE"),
            _column("improvement_surcharge", "DOUBLE"),
            _column("total_amount", "DOUBLE"),
            _column("congestion_surcharge", "DOUBLE"),
            _column("airport_fee", "DOUBLE"),
            _column("ehail_fee", "DOUBLE"),
            _column("trip_type", "DOUBLE"),
            _column("_ingestion_ts", "TIMESTAMP"),
            _column("_source_file", "STRING"),
            _column("_batch_id", "STRING"),
            _column("_rescued_data", "STRING"),
        ],
        "cluster_by": ("lpep_pickup_datetime",),
    },
    "trip_silver": {
        "table_name": "trips",
        "columns": [
            _column("VendorID", "INT"),
            _column("passenger_count", "INT"),
            _column("total_amount", "DECIMAL(10,2)"),
            _column("pickup_datetime", "TIMESTAMP"),
            _column("dropoff_datetime", "TIMESTAMP"),
            _column("tripsk", "STRING", nullable=False),
            _column("_ingestion_ts", "TIMESTAMP"),
        ],
        "cluster_by": ("pickup_datetime",),
    },
    "trip_silver_quarantine": {
        "table_name": "trips_quarantine",
        "columns": [
            _column("VendorID", "INT"),
            _column("passenger_count", "DOUBLE"),
            _column("total_amount", "DOUBLE"),
            _column("pickup_datetime", "TIMESTAMP"),
            _column("dropoff_datetime", "TIMESTAMP"),
            _column("_ingestion_ts", "TIMESTAMP"),
            _column("_quarantined_at", "TIMESTAMP"),
            _column("_rejection_reasons", "ARRAY<STRING>"),
        ],
        "cluster_by": (),
    },
    "dim_date": {
        "table_name": "dim_date",
        "columns": [
            _column("date_key", "INT"),
            _column("full_date", "DATE"),
            _column("year", "INT"),
            _column("quarter", "INT"),
            _column("month", "INT"),
            _column("day", "INT"),
            _column("day_of_week", "INT"),
            _column("week_of_year", "INT"),
            _column("is_weekend", "BOOLEAN"),
        ],
        "cluster_by": ("full_date",),
    },
    "dim_vendor": {
        "table_name": "dim_vendor",
        "columns": [
            _column("vendor_id", "INT"),
            _column("vendor_name", "STRING"),
            _column("vendor_short_name", "STRING"),
        ],
        "cluster_by": (),
    },
    "fact_trip": {
        "table_name": "fact_trip",
        "columns": [
            _column("tripsk", "STRING", nullable=False),
            _column("date_key", "INT"),
            _column("vendor_id", "INT"),
            _column("passenger_count", "INT"),
            _column("total_amount", "DECIMAL(10,2)"),
            _column("pickup_datetime", "TIMESTAMP"),
            _column("dropoff_datetime", "TIMESTAMP"),
            _column("duration_seconds", "LONG"),
            _column("taxi_type", "STRING"),
            _column("_ingestion_ts", "TIMESTAMP"),
        ],
        "cluster_by": ("pickup_datetime",),
    },
    "agg_trip_hourly_daily": {
        "table_name": "agg_trip_hourly_daily",
        "columns": [
            _column("date_key", "INT"),
            _column("pickup_hour", "INT"),
            _column("taxi_type", "STRING"),
            _column("trip_count", "LONG"),
            _column("total_amount_sum", "DECIMAL(18,2)"),
            _column("passenger_count_sum", "LONG"),
            _column("avg_total_amount", "DOUBLE"),
            _column("avg_passenger_count", "DOUBLE"),
        ],
        "cluster_by": ("date_key",),
    },
    "agg_trip_monthly_taxi": {
        "table_name": "agg_trip_monthly_taxi",
        "columns": [
            _column("month_key", "INT"),
            _column("year", "INT"),
            _column("month", "INT"),
            _column("taxi_type", "STRING"),
            _column("trip_count", "LONG"),
            _column("total_amount_sum", "DECIMAL(18,2)"),
            _column("passenger_count_sum", "LONG"),
            _column("avg_total_amount", "DOUBLE"),
            _column("avg_passenger_count", "DOUBLE"),
        ],
        "cluster_by": ("month_key",),
    },
}


def get_table_schema(schema_key):
    return TABLE_SCHEMAS[schema_key]


def get_schema_column_definitions(schema_key):
    return list(get_table_schema(schema_key)["columns"])


def get_schema_column_names(schema_key):
    return [column["name"] for column in get_schema_column_definitions(schema_key)]


def get_schema_columns(schema_key):
    return [
        (column["name"], column["dtype"])
        for column in get_schema_column_definitions(schema_key)
    ]


def get_schema_cluster_by(schema_key):
    return tuple(get_table_schema(schema_key).get("cluster_by", ()))


def render_ddl(schema_key, catalog, schema, table=None):
    table_schema = get_table_schema(schema_key)
    table_name = table or table_schema["table_name"]
    columns = get_schema_column_definitions(schema_key)
    cluster_by = get_schema_cluster_by(schema_key)
    column_width = max(len(column["name"]) for column in columns)
    columns_sql = ",\n".join(
        (
            f"    {column['name']:<{column_width}} {column['dtype']}"
            if column["nullable"]
            else f"    {column['name']:<{column_width}} {column['dtype']} NOT NULL"
        )
        for column in columns
    )
    cluster_sql = (
        f"\nCLUSTER BY ({', '.join(cluster_by)})"
        if cluster_by
        else ""
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} (\n"
        f"{columns_sql}\n"
        f")\n"
        f"USING DELTA{cluster_sql}\n"
    )
