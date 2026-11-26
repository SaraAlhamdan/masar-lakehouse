"""Capstone step 6: run the whole pipeline."""
import subprocess
import sys
from pathlib import Path


def run(cmd):
    print("+", " ".join(cmd), flush=True)
    if subprocess.run(cmd).returncode != 0:
        sys.exit(1)


def main():
    # 1. Land bronze
    if not Path("lakehouse/bronze/trips/_delta_log").exists():
        run([sys.executable, "-m", "masar.ingest.land_bronze"])

    # 2. Register bronze tables
    run([sys.executable, "-m", "masar.register"])

    # 3. Build silver
    run(["bash", "-c", "cd dbt/masar && dbt build"])

    # Quality check
    run([sys.executable, "-m", "masar.quality.contract", "silver"])

    # 4. Build gold marts
    run([sys.executable, "-c",
         "from masar.transform.build_gold import build_zone_hourly_demand; build_zone_hourly_demand()"])

    run([sys.executable, "-c",
         "from masar.transform.build_gold import build_trip_features; build_trip_features()"])

    # 5. Build consumer table
    run([sys.executable, "-m", "masar.serve.bi_view"])

    # 6. Print row counts
    from masar.spark import get_spark

    spark = get_spark("counts")

    for table in [
        "bronze/trips",
        "bronze/drivers",
        "bronze/gps_pings",
        "silver/trips",
        "gold/zone_hourly_demand",
        "gold/trip_features",
        "gold/bi_trips_safe"
    ]:
        count = spark.read.format("delta").load(
            f"lakehouse/{table}"
        ).count()
        print(f"{table}: {count:,}")

    spark.stop()


if __name__ == "__main__":
    main()