from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
conf = (
    SparkConf()
    .setAppName("Spark minIO Test")
    .set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .set("spark.hadoop.fs.s3a.access.key", 'admin')
    .set("spark.hadoop.fs.s3a.secret.key", 'admin1234')
    .set("spark.hadoop.fs.s3a.path.style.access", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
)
sc = SparkContext(conf=conf).getOrCreate()
sqlContext = SQLContext(sc)
sc.setLogLevel('INFO')
spark = SparkSession.builder.config(conf=conf).appName('abc').getOrCreate()

df = spark.read.option("header", "true").csv('s3a://data/ratings/rating.csv')
df.createOrReplaceTempView('ratings')
ratings = spark.sql("SELECT * from ratings limit 10").show()

ratings_avg = spark.sql(
    """SELECT *,
        COALESCE(mean(rating) OVER (
        PARTITION BY userId 
        ORDER BY CAST(timestamp AS timestamp) 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING 
        ),0) AS avg_ratings_previous,
        sum(1) OVER (
        PARTITION BY userId 
        ORDER BY CAST(timestamp AS timestamp) 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING 
        ) AS nb_previous_ratings
      FROM ratings where userId = 1""")