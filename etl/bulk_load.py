import asyncio
import aioredis
import boto3
import awswrangler as wr
import json


wr.config.s3_endpoint_url = "http://localhost:9000"

def get_boto3_session():
    session = boto3.session.Session(
        aws_access_key_id="admin",
        aws_secret_access_key="admin1234",
    )
    return session

def create_payload(row):
    key = row["userId"]
    value = {"avg_ratings_previous": str(row["avg_ratings_previous"]), "nb_previous_ratings": str(row["nb_previous_ratings"]) }
    return int(key), json.dumps(value)

async def go():
    r = await aioredis.create_redis_pool('redis://localhost:6379')
    data_stream = wr.s3.read_parquet(
        path="s3://data/to_kvs/",
        boto3_session=get_boto3_session(), chunked=100
    )
    for chunk in data_stream:
        for _, row in chunk.iterrows():
            k, v = create_payload(row)
            await r.set(k, v)
    r.close()
    await r.wait_closed()

asyncio.run(go())