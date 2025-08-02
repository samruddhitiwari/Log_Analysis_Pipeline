from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("LogProcessor") \
    .config("spark.jars", "postgresql-42.2.18.jar") \
    .getOrCreate()

logs_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "logs") \
    .load()

logs_df = logs_df.selectExpr("CAST(value AS STRING)")

parsed_df = logs_df.select(
    regexp_extract('value', r'(^[^ ]*)', 1).alias('ip'),
    regexp_extract('value', r'(\d{3})$', 1).alias('status_code'),
    current_timestamp().alias('timestamp')
)

parsed_df.writeStream \
    .foreachBatch(lambda df, _: df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/logs") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "log_table") \
        .option("user", "postgres") \
        .option("password", "yourpassword") \
        .mode("append") \
        .save()) \
    .start() \
    .awaitTermination()
