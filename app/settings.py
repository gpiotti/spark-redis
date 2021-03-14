import os

class AppConfig:
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
    KVS_INPUT_PATH="s3://data/to_kvs/"
    REDIS_URL='redis://localhost:6379'