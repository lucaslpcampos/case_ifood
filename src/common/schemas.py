"""Schemas estruturais e renderizacao de DDL das tabelas do pipeline."""


from pyspark.sql.session import SparkSession


def _column(
    name: str,
    dtype: str,
    nullable: bool = True,
    comment: str | None = None,
) -> dict[str, object]:
    """
    Cria a definicao padrao de uma coluna no registry de schemas.

    Args:
        name: Nome da coluna.
        dtype: Tipo SQL da coluna.
        nullable: Indica se a coluna aceita valores nulos.
        comment: Comentario opcional da coluna no Unity Catalog.

    Returns:
        Dicionario com os metadados estruturais da coluna.
    """
    return {
        "name": name,
        "dtype": dtype,
        "nullable": nullable,
        "comment": comment,
    }


TABLE_SCHEMAS = {
    "yellow_bronze": {
        "table_name": "yellow_trips",
        "columns": [
            _column("VendorID", "INT"),
            _column("tpep_pickup_datetime", "TIMESTAMP"),
            _column("tpep_dropoff_datetime", "TIMESTAMP"),
            _column("passenger_count", "INT"),
            _column("trip_distance", "DOUBLE"),
            _column("RatecodeID", "INT"),
            _column("store_and_fwd_flag", "STRING"),
            _column("PULocationID", "INT"),
            _column("DOLocationID", "INT"),
            _column("payment_type", "INT"),
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
        "cluster_by": (),
    },
    "green_bronze": {
        "table_name": "green_trips",
        "columns": [
            _column("VendorID", "INT"),
            _column("lpep_pickup_datetime", "TIMESTAMP"),
            _column("lpep_dropoff_datetime", "TIMESTAMP"),
            _column("passenger_count", "INT"),
            _column("trip_distance", "DOUBLE"),
            _column("RatecodeID", "INT"),
            _column("store_and_fwd_flag", "STRING"),
            _column("PULocationID", "INT"),
            _column("DOLocationID", "INT"),
            _column("payment_type", "INT"),
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
            _column("trip_type", "INT"),
            _column("_ingestion_ts", "TIMESTAMP"),
            _column("_source_file", "STRING"),
            _column("_batch_id", "STRING"),
            _column("_rescued_data", "STRING"),
        ],
        "cluster_by": (),
    },
    "trip_silver": {
        "table_name": "trips",
        "table_comment": (
            "Tabela Silver com viagens aprovadas nas regras de qualidade e prontas "
            "para consumo pela camada Gold."
        ),
        "columns": [
            _column("VendorID", "INT", comment="Identificador do vendor da corrida."),
            _column(
                "passenger_count",
                "INT",
                comment="Quantidade de passageiros informada para a viagem.",
            ),
            _column(
                "total_amount",
                "DECIMAL(10,2)",
                comment="Valor total recebido na corrida.",
            ),
            _column(
                "pickup_datetime",
                "TIMESTAMP",
                comment="Data e hora padronizadas de inicio da corrida.",
            ),
            _column(
                "dropoff_datetime",
                "TIMESTAMP",
                comment="Data e hora padronizadas de fim da corrida.",
            ),
            _column(
                "tripsk",
                "STRING",
                nullable=False,
                comment="Chave surrogate da viagem usada no merge incremental.",
            ),
            _column(
                "_ingestion_ts",
                "TIMESTAMP",
                comment="Timestamp tecnico da ingestao do registro no pipeline.",
            ),
        ],
        "cluster_by": ("pickup_datetime",),
    },
    "trip_silver_quarantine": {
        "table_name": "trips_quarantine",
        "table_comment": (
            "Tabela Silver de quarentena com registros rejeitados pelo ruleset "
            "de qualidade ou fora do contrato analitico do estudo."
        ),
        "columns": [
            _column("VendorID", "INT", comment="Identificador do vendor da corrida."),
            _column(
                "passenger_count",
                "DOUBLE",
                comment="Quantidade de passageiros recebida antes da reprovação.",
            ),
            _column(
                "total_amount",
                "DOUBLE",
                comment="Valor total recebido antes da reprovação na Silver.",
            ),
            _column(
                "pickup_datetime",
                "TIMESTAMP",
                comment="Data e hora padronizadas de inicio da corrida.",
            ),
            _column(
                "dropoff_datetime",
                "TIMESTAMP",
                comment="Data e hora padronizadas de fim da corrida.",
            ),
            _column(
                "_ingestion_ts",
                "TIMESTAMP",
                comment="Timestamp tecnico da ingestao original do registro.",
            ),
            _column(
                "_quarantined_at",
                "TIMESTAMP",
                comment="Timestamp em que o registro foi enviado para quarentena.",
            ),
            _column(
                "_rejection_reasons",
                "ARRAY<STRING>",
                comment="Lista de regras de qualidade violadas pelo registro.",
            ),
        ],
        "cluster_by": (),
    },
    "dim_date": {
        "table_name": "dim_date",
        "table_comment": (
            "Dimensao calendario usada para analises temporais no intervalo "
            "de estudo do case."
        ),
        "columns": [
            _column("date_key", "INT", comment="Chave numerica da data no formato yyyymmdd."),
            _column("full_date", "DATE", comment="Data calendario completa."),
            _column("year", "INT", comment="Ano da data calendario."),
            _column("quarter", "INT", comment="Trimestre da data calendario."),
            _column("month", "INT", comment="Mes da data calendario."),
            _column("day", "INT", comment="Dia do mes da data calendario."),
            _column("day_of_week", "INT", comment="Dia da semana conforme calendario Spark."),
            _column("week_of_year", "INT", comment="Semana do ano da data calendario."),
            _column("is_weekend", "BOOLEAN", comment="Indicador de final de semana."),
        ],
        "cluster_by": (),
    },
    "dim_vendor": {
        "table_name": "dim_vendor",
        "table_comment": "Dimensao de vendors dos taxis suportados pela analise.",
        "columns": [
            _column("vendor_id", "INT", comment="Identificador do vendor da corrida."),
            _column("vendor_name", "STRING", comment="Nome completo do vendor."),
            _column("vendor_short_name", "STRING", comment="Nome curto do vendor."),
        ],
        "cluster_by": (),
    },
    "fact_trip": {
        "table_name": "fact_trip",
        "table_comment": (
            "Fato transacional canonica da camada Gold no menor grao disponivel "
            "para analises de corridas."
        ),
        "columns": [
            _column(
                "tripsk",
                "STRING",
                nullable=False,
                comment="Chave surrogate unica da viagem usada em joins e merges.",
            ),
            _column("date_key", "INT", comment="Chave da dimensao de datas."),
            _column("vendor_id", "INT", comment="Chave do vendor associado a corrida."),
            _column(
                "passenger_count",
                "INT",
                comment="Quantidade de passageiros utilizada nas analises.",
            ),
            _column(
                "total_amount",
                "DECIMAL(10,2)",
                comment="Valor total recebido na corrida.",
            ),
            _column(
                "pickup_datetime",
                "TIMESTAMP",
                comment="Data e hora de inicio da corrida.",
            ),
            _column(
                "dropoff_datetime",
                "TIMESTAMP",
                comment="Data e hora de fim da corrida.",
            ),
            _column(
                "duration_seconds",
                "LONG",
                comment="Duracao da corrida em segundos.",
            ),
            _column(
                "taxi_type",
                "STRING",
                comment="Tipo de taxi de origem da viagem, como yellow ou green.",
            ),
            _column(
                "_ingestion_ts",
                "TIMESTAMP",
                comment="Timestamp tecnico herdado da ingestao do registro.",
            ),
        ],
        "cluster_by": ("taxi_type", "pickup_datetime"),
    },
    "agg_trip_hourly_daily": {
        "table_name": "agg_trip_hourly_daily",
        "table_comment": (
            "Agregado Gold por data, hora de pickup e tipo de taxi para "
            "analises de sazonalidade intradiaria."
        ),
        "columns": [
            _column("date_key", "INT", comment="Chave da dimensao de datas."),
            _column("pickup_hour", "INT", comment="Hora do dia em que a corrida iniciou."),
            _column("taxi_type", "STRING", comment="Tipo de taxi agregado na linha."),
            _column("trip_count", "LONG", comment="Quantidade de corridas no grao agregado."),
            _column(
                "total_amount_sum",
                "DECIMAL(18,2)",
                comment="Soma de total_amount no grao agregado.",
            ),
            _column(
                "passenger_count_sum",
                "LONG",
                comment="Soma de passageiros no grao agregado.",
            ),
            _column(
                "avg_total_amount",
                "DOUBLE",
                comment="Media de total_amount no grao agregado.",
            ),
            _column(
                "avg_passenger_count",
                "DOUBLE",
                comment="Media de passageiros no grao agregado.",
            ),
        ],
        "cluster_by": (),
    },
    "agg_trip_monthly_taxi": {
        "table_name": "agg_trip_monthly_taxi",
        "table_comment": (
            "Agregado Gold mensal por tipo de taxi para responder indicadores "
            "de valor total e passageiros no nivel de mes."
        ),
        "columns": [
            _column("month_key", "INT", comment="Chave numerica do mes no formato yyyymm."),
            _column("year", "INT", comment="Ano do agregado mensal."),
            _column("month", "INT", comment="Mes do agregado mensal."),
            _column("taxi_type", "STRING", comment="Tipo de taxi agregado na linha."),
            _column("trip_count", "LONG", comment="Quantidade de corridas no mes."),
            _column(
                "total_amount_sum",
                "DECIMAL(18,2)",
                comment="Soma de total_amount no mes e tipo de taxi.",
            ),
            _column(
                "passenger_count_sum",
                "LONG",
                comment="Soma de passageiros no mes e tipo de taxi.",
            ),
            _column(
                "avg_total_amount",
                "DOUBLE",
                comment="Media de total_amount no mes e tipo de taxi.",
            ),
            _column(
                "avg_passenger_count",
                "DOUBLE",
                comment="Media de passageiros no mes e tipo de taxi.",
            ),
        ],
        "cluster_by": (),
    },
}


def get_table_schema(schema_key: str) -> dict[str, object]:
    """
    Retorna a definicao estrutural de uma tabela registrada.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.

    Returns:
        Dicionario com metadados da tabela, colunas e clustering.
    """
    return TABLE_SCHEMAS[schema_key]


def get_schema_column_definitions(schema_key: str) -> list[dict[str, object]]:
    """
    Retorna a lista ordenada de definicoes de colunas de um schema.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.

    Returns:
        Lista de dicionarios com nome, tipo e nulabilidade das colunas.
    """
    return list(get_table_schema(schema_key)["columns"])


def get_schema_column_names(schema_key: str) -> list[str]:
    """
    Retorna apenas os nomes das colunas de um schema, na ordem declarada.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.

    Returns:
        Lista ordenada com os nomes das colunas.
    """
    return [column["name"] for column in get_schema_column_definitions(schema_key)]


def get_schema_columns(schema_key: str) -> list[tuple[str, str]]:
    """
    Retorna pares nome/tipo de coluna de um schema.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.

    Returns:
        Lista ordenada de tuplas no formato `(nome, tipo_sql)`.
    """
    return [
        (column["name"], column["dtype"])
        for column in get_schema_column_definitions(schema_key)
    ]


def get_schema_cluster_by(schema_key: str) -> tuple[str, ...]:
    """
    Retorna as colunas configuradas para clustering de um schema.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.

    Returns:
        Tupla com os nomes das colunas usadas em `CLUSTER BY`.
    """
    return tuple(get_table_schema(schema_key).get("cluster_by", ()))


def get_table_comment(schema_key: str) -> str | None:
    """
    Retorna o comentario configurado para uma tabela do registry.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.

    Returns:
        Comentario da tabela ou `None` quando nao houver metadata configurada.
    """
    return get_table_schema(schema_key).get("table_comment")


def get_schema_column_comments(schema_key: str) -> dict[str, str]:
    """
    Retorna os comentarios configurados para as colunas de um schema.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.

    Returns:
        Dicionario com nome da coluna e comentario associado.
    """
    return {
        column["name"]: column["comment"]
        for column in get_schema_column_definitions(schema_key)
        if column.get("comment")
    }


def _escape_sql_comment(comment: str) -> str:
    """
    Escapa aspas simples para uso seguro em DDL SQL de comentarios.

    Args:
        comment: Texto original do comentario.

    Returns:
        Texto do comentario com aspas simples escapadas para SQL.
    """
    return comment.replace("'", "''")


def render_ddl(
    schema_key: str,
    catalog: str,
    schema: str,
    table: str | None = None,
) -> str:
    """
    Gera o comando DDL de criacao da tabela a partir do registry de schemas.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.
        catalog: Nome do catalogo Unity Catalog.
        schema: Nome do schema onde a tabela sera criada.
        table: Nome opcional da tabela para sobrescrever o nome padrao do registry.

    Returns:
        String SQL com o `CREATE TABLE IF NOT EXISTS` da tabela.
    """
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


def render_comment_ddls(
    schema_key: str,
    catalog: str,
    schema: str,
    table: str | None = None,
) -> list[str]:
    """
    Gera os DDLs necessarios para comentar uma tabela e suas colunas.

    Args:
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.
        catalog: Nome do catalogo Unity Catalog.
        schema: Nome do schema onde a tabela existe.
        table: Nome opcional da tabela para sobrescrever o nome padrao do registry.

    Returns:
        Lista de comandos SQL de comentarios de tabela e coluna.
    """
    table_schema = get_table_schema(schema_key)
    table_name = table or table_schema["table_name"]
    table_fqn = f"{catalog}.{schema}.{table_name}"
    ddls: list[str] = []

    table_comment = get_table_comment(schema_key)
    if table_comment:
        ddls.append(
            f"COMMENT ON TABLE {table_fqn} IS '{_escape_sql_comment(table_comment)}'"
        )

    for column_name, column_comment in get_schema_column_comments(schema_key).items():
        ddls.append(
            f"ALTER TABLE {table_fqn} ALTER COLUMN {column_name} COMMENT "
            f"'{_escape_sql_comment(column_comment)}'"
        )

    return ddls


def apply_table_comments(
    spark: SparkSession,
    schema_key: str,
    catalog: str,
    schema: str,
    table: str | None = None,
) -> list[str]:
    """
    Aplica os comentarios configurados para uma tabela e suas colunas.

    Args:
        spark: Sessao Spark ativa.
        schema_key: Chave do schema registrada em `TABLE_SCHEMAS`.
        catalog: Nome do catalogo Unity Catalog.
        schema: Nome do schema onde a tabela existe.
        table: Nome opcional da tabela para sobrescrever o nome padrao do registry.

    Returns:
        Lista de DDLs executados para aplicacao dos comentarios.
    """
    ddls = render_comment_ddls(schema_key, catalog, schema, table)
    for ddl in ddls:
        spark.sql(ddl)
    return ddls
