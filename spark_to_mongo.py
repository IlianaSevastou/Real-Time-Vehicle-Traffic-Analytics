from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, StringType, IntegerType, DoubleType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WriteToMongoDB") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.0.5") \
    .getOrCreate()

# Function to write batch data to MongoDB
def write_to_mongo(batch_df, batch_id, collection_name):
    """Writes batch data to MongoDB collection."""
    if batch_df.count() > 0:
        batch_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("uri", f"mongodb://127.0.0.1/traffic.{collection_name}") \
            .save()

# Read and store raw Kafka messages

raw_schema = StructType() \
    .add("name", StringType()) \
    .add("dn", IntegerType()) \
    .add("orig", StringType()) \
    .add("dest", StringType()) \
.add("t", IntegerType()) \
    .add("link", StringType()) \
    .add("x", DoubleType()) \
    .add("s", DoubleType()) \
    .add("v", DoubleType())

raw_vehicle_data = spark.readStream \
    .format("parquet") \
    .schema(raw_schema) \
    .option("path", "/tmp/raw_vehicle_data") \
    .load()


raw_vehicle_data.writeStream \
    .foreachBatch(lambda batch_df, batch_id: write_to_mongo(batch_df, batch_id, "raw_data")) \
    .option("checkpointLocation", "/tmp/mongo_raw_checkpoint") \
    .start()

# Read and store processed Spark data
processed_schema = StructType() \
    .add("start", TimestampType()) \
    .add("end", TimestampType()) \
    .add("link", StringType()) \
    .add("vcount", IntegerType()) \
    .add("vspeed", DoubleType())
processed_vehicle_data = spark.readStream \
    .format("parquet") \
    .schema(processed_schema) \
    .option("path", "/tmp/processed_vehicle_data") \
    .load()


processed_vehicle_data.writeStream \
    .foreachBatch(lambda batch_df, batch_id: write_to_mongo(batch_df, batch_id, "processed_data")) \
    .option("checkpointLocation", "/tmp/mongo_processed_checkpoint") \
    .start() \
    .awaitTermination()
