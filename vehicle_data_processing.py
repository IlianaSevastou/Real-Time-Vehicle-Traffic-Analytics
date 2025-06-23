from pyspark.sql import SparkSession                                                                                                                        from pyspark.sql.functions import col, from_json, avg, count, window, to_timestamp                                                                                                                                                                                                                                      # Initialize Spark session                                                                                                                                  spark = SparkSession.builder \                                                                                                                                  .appName("VehicleDataProcessing") \                                                                                                                         .getOrCreate()                                                                                                                                                                                                                                                                                                      # Read data from Kafka                                                                                                                                      kafka_df = spark.readStream \                                                                                                                                   .format("kafka") \                                                                                                                                          .option("kafka.bootstrap.servers", "localhost:9092") \                                                                                                      .option("subscribe", "vehicle-positions") \                                                                                                                 .option("startingOffsets", "earliest") \                                                                                                                    .load()                                                                                                                                                                                                                                                                                                             # Parse Kafka messages as JSON (schema inferred automatically)                                                                                              vehicle_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value") \                                                                                       .select(from_json(col("json_value"), "STRUCT<name: STRING, origin: STRING, destination: STRING, time: STRING, link: STRING, position: FLOAT, spacing: F>    .select("data.*")  # Flatten the JSON structure

# Add a new column for simulation time (parsed from `time` field)
vehicle_df = vehicle_df.withColumn("sim_time", to_timestamp(col("time")))

vehicle_df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/raw_vehicle_data") \
    .option("checkpointLocation", "/tmp/raw_checkpoint") \
    .outputMode("append") \
    .start()

# Aggregations: Calculate vcount (vehicle count) and vspeed (average speed) per link
aggregated_df = vehicle_df \
    .withWatermark("sim_time", "5 minutes") \
    .groupBy(
        "link",
        window(col("sim_time"), "1 minute")
    ) \
    .agg(
        count("name").alias("vcount"),
        avg("speed").alias("vspeed")
    ) \
    .select("window.start", "window.end", "link", "vcount", "vspeed")

# Write the output to the console
query = aggregated_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

aggregated_df.writeStream \
    .format("parquet") \
    .option("path", "/tmp/processed_vehicle_data") \
    .option("checkpointLocation", "/tmp/processed_checkpoint") \
    .outputMode("append") \
    .start() \
    .awaitTermination()

query.awaitTermination()