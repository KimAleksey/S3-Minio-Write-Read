import duckdb
import logging

from pandas import DataFrame
from datetime import datetime

from utils.creds_utils import get_postgres_creds, get_minio_creds

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

MINIO_CREDS = get_minio_creds()
POSTGRES_CREDS = get_postgres_creds()


def extract_parquet_from_s3(
        bucket_name: str,
        object_name: str,
        conn_params: dict[str, str] | None = None,
) -> DataFrame:
    """
    Читает данные из S3 хранилища по заданному бакету и имени файла.

    :param bucket_name: Имя bucket.
    :param object_name: Имя файла (пути).
    :param conn_params:  Dict with Postgres connection parameters.
    :return: DataFrame
    """
    logging.info("Подключение к S3.")
    con = duckdb.connect()

    if conn_params is None:
        conn_params = MINIO_CREDS

    endpoint = conn_params["endpoint"]
    access_key = conn_params["access_key"]
    secret_key = conn_params["secret_key"]
    secure = conn_params.get("secure", False)

    logging.info("Подключение к S3 успешно.")
    try:
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
        logging.info(f"Успешно получены данные из S3: {object_name}")
    except Exception as e:
        logging.error(e)
        logging.error("DataFrame - empty.")
        return DataFrame()

    con.close()
    return df


def transform_df(df: DataFrame, object_name: str = "") -> DataFrame:
    """
    Добавляем системные столбцы.

    :param df: DataFrame
    :param object_name: Путь файла. Или имя.
    :return: DataFrame с дополнительными полями.
    """
    if df.empty:
        logging.error("DataFrame - empty.")
        return df

    logging.info("Добавляем системные поля в DataFrame.")
    df["ingested_at"] = datetime.now()
    df["source_system"] = "s3"
    df["source_file"] = object_name
    logging.info("Системные поля добавлены.")
    return df


def load_df_to_postgres(
        table: str,
        df: DataFrame,
        object_name: str,
        conn_params: dict[str, str] | None = None,
        schema: str = "raw"
) -> None:
    """
    Загружает данные по заданному файлу, таблице используя данные DataFrame.

    :param table: Целевая таблица.
    :param df: Целевые данные.
    :param object_name: Файл, данные которого перезагружаются.
    :param schema: Целевая схема.
    :param conn_params: Dict with Postgres connection parameters.
    :return: None.
    """
    if df.empty:
        logging.error("DataFrame - empty. Данные не загружены.")
        return None

    # Устанавливаем параметры соединения
    if conn_params is None:
        conn_params = POSTGRES_CREDS

    # Подключаем DuckDB
    con = duckdb.connect()
    logging.info("Подключение к Postgres.")

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
    logging.info("Подключение к Postgres успешно.")

    # Определяем View
    con.register("input_df", df)

    # Создаем схему
    con.execute(f"""
        CREATE SCHEMA IF NOT EXISTS pg.{schema};
    """)

    # Создаем таблицу, если не создана. Загружаем или перегружаем данные.
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS pg.{schema}.{table} 
        AS SELECT * FROM input_df LIMIT 0;

        DELETE FROM pg.{schema}.{table} WHERE source_system = '{object_name}';
        INSERT INTO pg.{schema}.{table} SELECT * FROM input_df;
    """)
    logging.info(f"Данные успешно перегружены в таблице: {table}.")