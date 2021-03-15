import os


class AppConfig:
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
    KVS_INPUT_PATH = "s3://data/to_kvs/"
    REDIS_HOST = os.getenv("REDIS_HOST")
    REDIS_PORT = os.getenv("REDIS_PORT")
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}"
