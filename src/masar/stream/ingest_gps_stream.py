from pyspark.sql import functions as F
from masar.config import LAKEHOUSE
from masar.spark import get_spark
from masar.stream.schemas import PING_SCHEMA

BRONZE = str(LAKEHOUSE / "bronze" / "gps_stream")
CHECKPOINT = str(LAKEHOUSE / "_checkpoints" / "gps_bronze")


def run() -> None:
    spark = get_spark("masar-gps-ingest", kafka=True)

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "masar.gps.pings")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )

    parsed = (
        raw
        .select(
            F.from_json(
                F.col("value").cast("string"),
                PING_SCHEMA
            ).alias("p"),
            F.col("timestamp").alias("_processing_ts")
        )
        .select("p.*", "_processing_ts")
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start(BRONZE)
        .awaitTermination()
    )


if __name__ == "__main__":
    run()
    