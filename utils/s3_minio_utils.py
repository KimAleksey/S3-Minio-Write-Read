import logging
import re

from minio import Minio
from minio.datatypes import Bucket

from utils.creds_utils import get_minio_creds


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
        file_name: str,
        url: str,
        file_pah: str = "./",
        part_size: int = 1024
):
    """
    Загружает файл по URL в bucket S3.

    :param client: Объект Minio.
    :param bucket_name: Имя bucket.
    :param file_name:
    :param url:
    :param file_pah:
    :param part_size:
    :return:
    """


if __name__ == "__main__":
    print(MINIO_CREDS)
    client = get_minio_client()
    create_bucket(client, "nyc-taxi-data")
    bucket_list = get_bucket_list(client)
    print(bucket_list)
    remove_bucket(client, "nyc-taxi-data")
    bucket_list = get_bucket_list(client)
    print(bucket_list)

