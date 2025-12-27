from typing import Any

from dotenv import load_dotenv
from os import getenv
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / "conf" / ".env"
load_dotenv(dotenv_path=env_path)

CREDS = {
    "minio": {
        "endpoint": getenv("MINIO_ENDPOINT"),
        "access_key": getenv("MINIO_ACCESS_KEY"),
        "secret_key": getenv("MINIO_SECRET_KEY"),
    },
    "postgres": {
        "host": getenv("POSTGRES_HOST"),
        "port": getenv("POSTGRES_PORT"),
        "database": getenv("POSTGRES_DB"),
        "login": getenv("POSTGRES_LOGIN"),
        "password": getenv("POSTGRES_PASSWORD"),
    },
}


def get_minio_creds() -> dict[str, Any]:
    """
    Возвращает параметры подключения к MINIO.

    :return: dict - параметры подключения к MINIO.
    """
    return CREDS["minio"].copy()


def get_postgres_creds() -> dict[str, Any]:
    """
    Возвращает параметры подключения к POSTGRES.

    :return: dict - параметры подключения к POSTGRES.
    """
    return CREDS["postgres"].copy()


if __name__ == "__main__":
    print(get_minio_creds())
    print(get_postgres_creds())