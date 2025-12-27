import logging
import re
import requests

from time import time

from minio import Minio
from minio.datatypes import Bucket

from utils.creds_utils import get_minio_creds

from typing import Any

# Конфигурация логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# Параметры подключения
MINIO_CREDS = get_minio_creds()

def is_valid_bucket_name(name: str) -> bool:
    """
    Правила для имя bucket:
        | Правило                                      | Пояснение                      |
        | -------------------------------------------- | ------------------------------ |
        | Длина 3–63 символа                           | Меньше 3 и больше 63 — нельзя  |
        | Только lowercase                             | Только `a–z`, `0–9`, `.` и `-` |
        | Начинается и заканчивается буквой или цифрой | Не `-bucket`, не `bucket-`     |
        | Не похоже на IP                              | `192.168.0.1` запрещено        |
        | Без подчёркиваний `_`                        | `_` запрещён                   |
        | Без заглавных букв                           | `MyBucket` запрещено           |
        | Без пробелов                                 | `my bucket` запрещено          |
        | Точки нельзя использовать с SSL wildcard     | Лучше избегать `.` вообще      |
        | Не может быть `xn--` в начале                | Зарезервировано для IDN        |
        | Не может быть `-s3alias` в конце             | Зарезервировано                |

    :param name: Имя bucket.
    :return: Валидное или нет.
    """
    if len(name) < 3 or len(name) > 63:
        return False
    if not re.fullmatch(r"[a-z0-9][a-z0-9\-\.]*[a-z0-9]", name):
        return False
    if "_" in name:
        return False
    # Проверка на IP-адрес
    if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", name):
        return False
    if name.startswith("xn--"):
        return False
    if name.endswith("-s3alias"):
        return False
    return True


def get_minio_client(secure: bool | None = False) -> Minio:
    """
    Устанавливает подключение к Minio.

    :param secure: Использовать TSL-соединение. По-умолчанию - False.
    :return: Объект Minio, для взаимодействия с Minio.
    """
    if not secure:
        secure = False
    return Minio(
        endpoint=MINIO_CREDS["endpoint"],
        access_key=MINIO_CREDS["access_key"],
        secret_key=MINIO_CREDS["secret_key"],
        secure=secure,
    )


def get_bucket_list(client: Minio) -> list[Bucket]:
    """
    Возвращает список bucket.

    :param client: Объект Minio.
    :return: Список bucket.
    """
    if not isinstance(client, Minio):
        raise TypeError("Minio must be an instance of Minio.")
    return client.list_buckets()


def create_bucket(client: Minio, name: str) -> bool:
    """
    Создает bucket если такой отсутствует.

    :param client: Объект Minio.
    :param name: Имя bucket.
    :return: False, если не получилось создать, иначе True.
    """
    if not isinstance(client, Minio):
        raise TypeError("Minio must be an instance of Minio.")
    if not isinstance(name, str):
        raise TypeError("Bucket name must be a string.")
    if not is_valid_bucket_name(name):
        raise ValueError("Невалидное имя для bucket.")
    if client.bucket_exists(name):
        logging.info("Bucket %s already exists.", name)
        return True
    try:
        client.make_bucket(name)
        logging.info("Bucket %s created.", name)
        return True
    except Exception as e:
        raise RuntimeError(f"Error while creating bucket") from e


def remove_bucket(client: Minio, name: str) -> bool:
    """
    Удаляет bucket если такой отсутствует.

    :param client: Объект Minio.
    :param name: Имя bucket.
    :return: False, если не получилось удалить, иначе True.
    """
    if not isinstance(client, Minio):
        raise TypeError("Minio must be an instance of Minio.")
    if not isinstance(name, str):
        raise TypeError("Bucket name must be a string.")
    if client.bucket_exists(name):
        client.remove_bucket(name)
        return True
    logging.info("Bucket %s not found.", name)
    return False


def load_data_to_bucket_via_url(
        client: Minio,
        bucket_name: str,
        file_path: str,
        url: str,
        part_size: int = 50 * 1024 * 1024
) -> bool:
    """
    Загружает файл по URL в bucket S3.

    :param client: Объект Minio.
    :param bucket_name: Имя bucket.
    :param file_path: Путь файла. Или просто имя файла
    :param url: Адрес, на котором лежит файл.
    :param part_size: Размер для батча. По-умолчанию 50MB
    :return: True если файл загружен, иначе False.
    """
    if not isinstance(client, Minio):
        raise TypeError("Minio must be an instance of Minio.")
    if not isinstance(bucket_name, str):
        raise TypeError("Bucket name must be a string.")
    if not isinstance(file_path, str):
        raise TypeError("File name must be a string.")
    if not isinstance(url, str):
        raise TypeError("URL must be a string.")
    if not client.bucket_exists(bucket_name):
        logging.info("Bucket %s not found.", bucket_name)

    # INFO - Начало загрузки
    start = time()
    logging.info(f"Start of downloading {file_path}.")
    logging.info(f"Bucket: {bucket_name}, file name: {file_path}, URL: {url}.")

    try:
        response = requests.get(url, stream=True, timeout=60)
    except Exception as e:
        logging.error(f"Error while downloading {file_path}.")
        return False

    # Проверка статуса = ОК
    if response.status_code != 200:
        logging.error(f"Error while downloading {file_path}.")
        return False

    # Размер файла
    length = int(response.headers.get("Content-Length", -1))
    try:
        client.put_object(
            bucket_name=bucket_name,
            object_name=file_path,
            length=length,
            data=response.raw,
            part_size=part_size,
            content_type=response.headers.get("Content-Type", "application/octet-stream"),
        )
    except Exception as e:
        logging.error(f"Error while uploading {file_path}.")
        return False

    # INFO - Конец загрузки
    end = time()
    logging.info(f"Downloaded {file_path} into bucket: {bucket_name}.")
    logging.info(f"End of downloading {file_path}.")
    logging.info(f"{file_path} was uploaded in {round((end - start), 2)} seconds.")
    return True

def get_data_from_bucket(
        client: Minio,
        bucket_name: str,
        file_path: str,
) -> Any:
    """
    Получаем файл по URL в bucket S3.

    :param client: Объект Minio.
    :param bucket_name: Имя bucket.
    :param file_path: Путь файла. Или просто имя файла
    :return: True если файл загружен, иначе False.
    """
    if not isinstance(client, Minio):
        raise TypeError("Minio must be an instance of Minio.")
    if not isinstance(bucket_name, str):
        raise TypeError("Bucket name must be a string.")
    if not isinstance(file_path, str):
        raise TypeError("File name must be a string.")
    if not client.bucket_exists(bucket_name):
        logging.info("Bucket %s not found.", bucket_name)
        return False

    try:
        file = client.get_object(bucket_name, file_path).data
    except Exception as e:
        raise RuntimeError(f"Error while getting {file_path}.")
    logging.info(f"Успешно получены данные файла: {file_path}.")
    return file


if __name__ == "__main__":
    print(MINIO_CREDS)