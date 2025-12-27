import duckdb

from pandas import DataFrame

from datetime import datetime

def extract_parquet_from_s3(conn_params: dict[str, str], bucket_name: str, object_name: str) -> DataFrame:
    """
    Читает данные из S3 хранилища по заданному бакету и имени файла.

    :param conn_params:  Dict with Postgres connection parameters.
    :param bucket_name: Имя bucket.
    :param object_name: Имя файла (пути).
    :return: DataFrame
    """
    con = duckdb.connect()
    endpoint = conn_params["endpoint"]
    access_key = conn_params["access_key"]
    secret_key = conn_params["secret_key"]
    secure = conn_params.get("secure", False)
    df = con.sql(
        f"""
        SET TIMEZONE = 'UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = '{endpoint}';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_use_ssl = {secure};
        SELECT * FROM read_parquet('s3://{bucket_name}/{object_name}');
        """,
    ).df()
    con.close()
    return df


def transform_df(df: DataFrame, object_name: str = "") -> DataFrame:
    """
    Добавляем системные столбцы.

    :param df: DataFrame
    :param object_name: Путь файла. Или имя.
    :return: DataFrame с дополнительными полями.
    """
    df["ingested_at"] = datetime.now()
    df["source_system"] = "s3"
    df["source_file"] = object_name
    return df


def load_df_to_postgres(
        conn_params: dict[str, str],
        table: str,
        df: DataFrame,
        object_name: str,
        schema: str = "raw"
) -> None:
    """
    Загружает данные по заданному файлу, таблице используя данные DataFrame.

    :param conn_params: Dict with Postgres connection parameters.
    :param table: Целевая таблица.
    :param df: Целевые данные.
    :param object_name: Файл, данные которого перезагружаются.
    :param schema: Целевая схема.
    :return: None.
    """
    # Подключаем DuckDB
    con = duckdb.connect()

    # Установка нужных коннекторов
    con.execute("""
        INSTALL postgres; 
        LOAD postgres;
    """)

    # Подключение к Postgres
    con.execute(f"""
    ATTACH 
        'host={conn_params["host"]} 
         port={conn_params["port"]} 
         user={conn_params["login"]} 
         password={conn_params["password"]} 
         dbname={conn_params["database"]}' 
    AS pg (TYPE postgres);
    """)

    # Определяем View
    con.register("input_df", df)

    # Создаем схему, если не создана. Загружаем или перегружаем данные.
    con.execute(f"""
        CREATE SCHEMA IF NOT EXISTS {schema};
        CREATE TABLE IF NOT EXISTS {schema}.{table};
        
        DELETE FROM {schema}.{table} WHERE source_system = '{object_name}';
        
        INSERT INTO pg.{schema}.{table} SELECT * FROM input_df;
    """)