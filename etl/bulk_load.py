import asyncio
import json
import logging
import os

import aioredis
import awswrangler as wr
import boto3

logging.getLogger().setLevel(logging.INFO)

MINIO_USER = os.getenv("MINIO_USER")
MINIO_PASS = os.getenv("MINIO_PASS")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_INPUT_PATH = os.getenv("KVS_INPUT_PATH")
REDIS_URL = os.getenv("REDIS_URL")


wr.config.s3_endpoint_url = S3_ENDPOINT


def get_boto3_session():
    session = boto3.session.Session(
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
    )
    return session


def create_payload(row):
    key = row["userId"]
    value = {
        "avg_ratings_previous": str(row["avg_ratings_previous"]),
        "nb_previous_ratings": str(row["nb_previous_ratings"]),
    }
    return int(key), json.dumps(value)


async def go():
    r = await aioredis.create_redis_pool(REDIS_URL)
    data_stream = wr.s3.read_parquet(
        path=S3_INPUT_PATH, boto3_session=get_boto3_session(), chunked=100
    )
    for ix, chunk in enumerate(data_stream):
        logging.info("Loading KVS")
        if ix % 50 == 0 and ix > 0:
            logging.info("Loaded {ix+1} users...")
        for _, row in chunk.iterrows():
            print(row)
            k, v = create_payload(row)
            await r.set(k, v)
    r.close()
    logging.info("Finished loading KVS")
    await r.wait_closed()


asyncio.run(go())
