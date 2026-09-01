"""Multi-source traffic pipeline using Spark Structured Streaming and Isolation Forest."""

from __future__ import annotations

import logging
import math
import os
import random

import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession, functions as F, types as T
from synapse.ml.isolationforest import IsolationForest


logging.basicConfig(level=logging.INFO, format="%(asctime)s | Spark | %(levelname)s | %(message)s")
LOGGER = logging.getLogger("spark-processor")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CHECKPOINT = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoint/traffic-iot-v1")
IOT_RAW_TOPIC = "traffic.iot.raw"
VIDEO_RAW_TOPIC = "traffic.video.raw"
IOT_PROCESSED_TOPIC = "traffic.iot.processed"
VIDEO_PROCESSED_TOPIC = "traffic.video.processed"
SPARK_VERSION = pyspark.__version__
SYNAPSEML_VERSION = "1.1.3"

RAW_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("source", T.StringType()),
        T.StructField("sensor_id", T.StringType()),
        T.StructField("road_id", T.StringType()),
        T.StructField("road_name", T.StringType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("timestamp", T.StringType()),
        T.StructField("speed_kmh", T.DoubleType()),
        T.StructField("density_pct", T.DoubleType()),
        T.StructField("occupancy_pct", T.DoubleType()),
        T.StructField("vehicle_count", T.IntegerType()),
        T.StructField("scenario", T.StringType()),
        T.StructField("cycle", T.IntegerType()),
        T.StructField("quality", T.DoubleType()),
        T.StructField(
            "vehicles",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("track_id", T.IntegerType()),
                        T.StructField("vehicle_type", T.StringType()),
                        T.StructField("speed_kmh", T.DoubleType()),
                        T.StructField("confidence", T.DoubleType()),
                        T.StructField("bbox", T.ArrayType(T.IntegerType())),
                    ]
                )
            ),
        ),
        T.StructField("inference_ms", T.DoubleType()),
        T.StructField("video_frame", T.LongType()),
        T.StructField("source_uri", T.StringType()),
        T.StructField("measurement_method", T.StringType()),
    ]
)


def build_spark() -> SparkSession:
    packages = ",".join(
        [
            f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}",
            f"com.microsoft.azure:synapseml-core_2.12:{SYNAPSEML_VERSION}",
        ]
    )
    return (
        SparkSession.builder.appName("RealtimeMultiSourceTrafficProcessor")
        .master("local[*]")
        .config("spark.jars.packages", packages)
        .config("spark.jars.repositories", "https://mmlspark.blob.core.windows.net/maven,https://repo1.maven.org/maven2")
        .config(
            "spark.jars.excludes",
            "org.scala-lang:scala-reflect,org.apache.spark:spark-tags_2.12,org.scalactic:scalactic_2.12,org.scalatest:scalatest_2.12,com.fasterxml.jackson.core:jackson-databind",
        )
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )


def make_training_rows(size: int = 1200) -> list[tuple[float, float, float, float]]:
    """Create a repeatable baseline of plausible normal traffic for model training."""
    rng = random.Random(2026)
    rows: list[tuple[float, float, float, float]] = []
    for index in range(size):
        phase = math.sin(index / 51) * .12
        density = max(8, min(68, rng.gauss(34 + phase * 30, 10)))
        speed = max(24, min(72, rng.gauss(56 - density * .36, 5)))
        vehicle_count = max(3, min(78, rng.gauss(density * .72, 7)))
        occupancy = max(5, min(75, density * rng.uniform(.72, .96)))
        rows.append((float(speed), float(density), float(vehicle_count), float(occupancy)))
    return rows


def train_model(spark: SparkSession) -> Pipeline:
    training_schema = "speed_kmh double, density_pct double, vehicle_count double, occupancy_pct double"
    training = spark.createDataFrame(make_training_rows(), training_schema)
    assembler = VectorAssembler(
        inputCols=["speed_kmh", "density_pct", "vehicle_count", "occupancy_pct"],
        outputCol="features",
        handleInvalid="keep",
    )
    detector = (
        IsolationForest()
        .setNumEstimators(100)
        .setBootstrap(False)
        .setMaxSamples(256)
        .setMaxFeatures(1.0)
        .setFeaturesCol("features")
        .setPredictionCol("predictedLabel")
        .setScoreCol("outlierScore")
        .setContamination(0.035)
        .setContaminationError(0.001)
        .setRandomSeed(2026)
    )
    LOGGER.info("Training SynapseML Isolation Forest on %d baseline records", training.count())
    return Pipeline(stages=[assembler, detector]).fit(training)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    model = train_model(spark)
    LOGGER.info("Isolation Forest ready; starting Kafka Structured Streaming query")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("subscribe", f"{IOT_RAW_TOPIC},{VIDEO_RAW_TOPIC}")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "5000")
        .load()
    )

    parsed = (
        kafka_stream.select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("data"), F.col("timestamp").alias("kafka_timestamp"))
        .select("data.*", "kafka_timestamp")
        .filter(F.col("source").isin("iot", "video") & F.col("road_id").isNotNull())
        .withColumn("event_time", F.coalesce(F.to_timestamp("timestamp"), F.col("kafka_timestamp")))
        .withColumn("speed_kmh", F.coalesce(F.col("speed_kmh"), F.lit(0.0)).cast("double"))
        .withColumn("density_pct", F.coalesce(F.col("density_pct"), F.lit(0.0)).cast("double"))
        .withColumn("vehicle_count", F.coalesce(F.col("vehicle_count"), F.lit(0)).cast("double"))
        .withColumn("occupancy_pct", F.coalesce(F.col("occupancy_pct"), F.col("density_pct") * F.lit(0.85)).cast("double"))
    )

    scored = model.transform(parsed)
    risk_score = F.least(
        F.lit(100.0),
        F.greatest(
            F.lit(0.0),
            F.col("density_pct") * F.lit(0.58)
            + (F.lit(65.0) - F.col("speed_kmh")) * F.lit(0.62)
            + F.col("outlierScore") * F.lit(16.0),
        ),
    )

    enriched = (
        scored.withColumn("anomaly", F.col("predictedLabel") >= F.lit(1.0))
        .withColumn("risk_score", F.round(risk_score, 1))
        .withColumn(
            "congestion_level",
            F.when((F.col("density_pct") >= 80) & (F.col("speed_kmh") < 18), "critical")
            .when((F.col("density_pct") >= 60) & (F.col("speed_kmh") < 28), "heavy")
            .when((F.col("density_pct") >= 40) | (F.col("speed_kmh") < 38), "moderate")
            .otherwise("smooth"),
        )
        .withColumn(
            "prediction_label",
            F.when(F.col("congestion_level") == "critical", "Nguy cơ ùn tắc nghiêm trọng")
            .when(F.col("congestion_level") == "heavy", "Có khả năng ùn tắc")
            .when(F.col("congestion_level") == "moderate", "Mật độ đang tăng")
            .otherwise("Giao thông ổn định"),
        )
        .withColumn(
            "alert_title",
            F.when(F.col("anomaly"), "Phát hiện biến động bất thường")
            .when(F.col("congestion_level") == "critical", "Tốc độ giảm nghiêm trọng")
            .when(F.col("congestion_level") == "heavy", "Mật độ giao thông cao")
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    output_columns = [
        "event_id", "source", "sensor_id", "road_id", "road_name", "latitude", "longitude",
        F.date_format("event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("timestamp"),
        F.round("speed_kmh", 2).alias("speed_kmh"), F.round("density_pct", 2).alias("density_pct"),
        F.round("occupancy_pct", 2).alias("occupancy_pct"), F.col("vehicle_count").cast("integer").alias("vehicle_count"),
        "scenario", "cycle", "quality", "congestion_level", "prediction_label", "anomaly",
        F.round("outlierScore", 4).alias("anomaly_score"), "risk_score", "alert_title",
        "vehicles", "inference_ms", "video_frame", "source_uri", "measurement_method",
        F.date_format("processed_at", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("processed_at"),
    ]

    kafka_output = enriched.select(
        F.when(F.col("source") == "video", F.lit(VIDEO_PROCESSED_TOPIC))
        .otherwise(F.lit(IOT_PROCESSED_TOPIC))
        .alias("topic"),
        F.col("road_id").cast("string").alias("key"),
        F.to_json(F.struct(*output_columns)).alias("value"),
    )

    query = (
        kafka_output.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP)
        .option("checkpointLocation", CHECKPOINT)
        .outputMode("append")
        .trigger(processingTime="2 seconds")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
