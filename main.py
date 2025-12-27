from utils.s3_minio_utils import get_minio_client
from utils.s3_minio_utils import create_bucket
from utils.s3_minio_utils import load_data_to_bucket_via_url
from utils.duckdb_utils import extract_parquet_from_s3
from utils.duckdb_utils import transform_df
from utils.duckdb_utils import load_df_to_postgres

YEAR = 2025
BUCKET_NAME = "nyc-taxi-data"
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def main():
    # 1. Запуск скрипта для загрузки данных в S3 Minio.
    client = get_minio_client()
    create_bucket(client, BUCKET_NAME)

    # Загружаем данные в S3
    files_loaded = []
    for i in range(1, 13):
        filename = f"yellow_tripdata_{YEAR}-{i:02}.parquet"
        filepath = f"{YEAR}/{i:02}/" + filename
        files_loaded.append(filepath)
        url = f"{BASE_URL}{filename}"
        load_data_to_bucket_via_url(client, BUCKET_NAME, filepath, url)

    # 2. Загружаем данные из S3
    for file in files_loaded:
        df = extract_parquet_from_s3(BUCKET_NAME, file)
        df = transform_df(df)
        load_df_to_postgres(
            table="nyc_taxi_data_2025",
            df=df,
            object_name=file,
        )

if __name__ == "__main__":
    main()