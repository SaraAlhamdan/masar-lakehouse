from pyspark.sql import functions as F
from masar.spark import get_spark


def profile(table_path: str) -> None:
    spark = get_spark("profile")
    df = spark.read.format("delta").load(table_path)

    n = df.count()
    print(f"rows={n:,} columns={len(df.columns)}")

    if n > 0:
        df.select([
            (F.sum(F.col(c).isNull().cast("int")) / F.lit(n)).alias(c)
            for c in df.columns
        ]).show(truncate=False, vertical=True)

    if "trip_id" in df.columns:
        d = df.groupBy("trip_id").count().filter("count > 1").count()
        print(f"duplicate trip_id groups: {d:,}")

    # Fare profiling
    if "fare_sar" in df.columns:
        df.select(
            F.min(F.col("fare_sar").cast("double")).alias("min_fare"),
            F.max(F.col("fare_sar").cast("double")).alias("max_fare"),
            F.avg(F.col("fare_sar").cast("double")).alias("avg_fare")
        ).show()

    # Ratings profiling
    if "rating" in df.columns:
        df.select(
            F.min(F.col("rating").cast("double")).alias("min_rating"),
            F.max(F.col("rating").cast("double")).alias("max_rating"),
            F.avg(F.col("rating").cast("double")).alias("avg_rating")
        ).show()


if __name__ == "__main__":
    profile("./lakehouse/bronze/trips")
    profile("./lakehouse/bronze/ratings")