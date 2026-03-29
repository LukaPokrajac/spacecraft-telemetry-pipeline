from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, window


def main():
    spark = (
        SparkSession.builder.appName("TelemetrySparkPractice")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # Practice job: read cleaned telemetry CSV export and compute windowed metrics.
    df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("sample_data/cleaned_telemetry_sample.csv")
    )

    df = df.withColumn("event_timestamp", col("event_timestamp").cast("timestamp"))
    agg = (
        df.groupBy(window(col("event_timestamp"), "1 minute"), col("spacecraft_id"))
        .agg(
            avg("temperature").alias("avg_temperature"),
            avg("vibration").alias("avg_vibration"),
            avg("battery_voltage").alias("avg_battery_voltage"),
        )
        .orderBy("window")
    )

    agg.show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
