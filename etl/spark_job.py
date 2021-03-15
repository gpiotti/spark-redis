import logging

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

logging.getLogger().setLevel(logging.INFO)
conf = (
    SparkConf()
    .setAppName("Spark minIO Test")
    .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .set("spark.hadoop.fs.s3a.access.key", "admin")
    .set("spark.hadoop.fs.s3a.secret.key", "admin1234")
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

RAW_INPUT_PATH = "s3a://data/raw_input/"
RATINGS_OUTPUT_PATH = "s3a://data/ratings/"
TO_KVS_PATH = "s3a://data/to_kvs/"

logging.info(
    "Running ratings calculation with the following params:\n"
    + f"RAW_INPUT_PATH: {RAW_INPUT_PATH}\n"
    + f"RATINGS_OUTPUT_PATH: {RATINGS_OUTPUT_PATH}\n"
    + f"TO_KVS_PATH: {TO_KVS_PATH}\n"
)
schema = StructType(
    [
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

ratings_raw = spark.read.csv(RAW_INPUT_PATH, header=True, schema=schema)

ratings_raw.createOrReplaceTempView("ratings_raw")

ratings_calc = spark.sql(
    """
select *,
COALESCE(sum(1) OVER (PARTITION BY userID 
             ORDER BY timestamp
             ROWS BETWEEN UNBOUNDED PRECEDING 
             AND 1 PRECEDING), 0)  as nb_previous_ratings,
avg(rating) OVER (PARTITION BY userID 
             ORDER BY timestamp
             ROWS BETWEEN UNBOUNDED PRECEDING 
             AND 1 PRECEDING)  as avg_ratings_previous

from ratings_raw where userId = 1
"""
)

ratings_calc.write.save(RATINGS_OUTPUT_PATH, format="parquet", mode="overwrite")
logging.info("Rating calcuations done")

ratings_calc.createOrReplaceTempView("ratings_calc")

logging.info("Calculating features...")
to_kvs = spark.sql(
    """
    select  userId,
    nb_previous_ratings,
    avg_ratings_previous
    from ratings_calc a
    where timestamp = ( select max(timestamp) 
                        from ratings_calc 
                        where userId = a.userId)
"""
)

to_kvs.write.save(TO_KVS_PATH, format="parquet", mode="overwrite")
logging.info("Calculating features...Done")
