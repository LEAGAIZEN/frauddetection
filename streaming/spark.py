import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, avg, sum as _sum
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

KAFKA_BROKER = os.environ["KAFKA_BROKER"]
TOPIC = os.environ["KAFKA_TOPIC"]

PG_HOST = os.environ["POSTGRES_HOST"]
PG_PORT = os.environ["POSTGRES_PORT"]
PG_DB = os.environ["POSTGRES_DB"]
PG_USER = os.environ["POSTGRES_USER"]
PG_PASSWORD = os.environ["POSTGRES_PASSWORD"]

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

schema = StructType(
    [StructField("Time", DoubleType())]
    + [StructField(f"V{i}", DoubleType()) for i in range(1, 29)]
    + [StructField("Amount", DoubleType()), StructField("Class", IntegerType())]
)


def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    renamed = batch_df.withColumnRenamed("Time", "time_seconds")
    renamed = renamed.toDF(*[c.lower() for c in renamed.columns])
    (
        renamed.write.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "transactions")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def main():
    spark = (
        SparkSession.builder
        .appName("FraudFeatureEngineering")
         .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.postgresql:postgresql:42.7.3"
    )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp"),
    ).select("data.*", "kafka_timestamp")

    features = (
        parsed
        .withWatermark("kafka_timestamp", "30 seconds")
        .groupBy(window(col("kafka_timestamp"), "10 seconds"))
        .agg(
            count("*").alias("txn_count"),
            avg("Amount").alias("avg_amount"),
            _sum("Class").alias("fraud_count"),
        )
    )

    query = (
        features.writeStream.format("console")
        .outputMode("update")
        .option("truncate", False)
        .trigger(processingTime="10 seconds")
        .start()
    )

    pg_query = (
        parsed.writeStream.foreachBatch(write_to_postgres)
        .option("checkpointLocation", "checkpoints/postgres")
        .trigger(processingTime="10 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()