import os
import shutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType, TimestampType

# Spark set-up settings
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.adaptive.enable", "false") \
    .getOrCreate()

# Function to clear contents of a checkpoint directory
def clear_checkpoint_directory(checkpoint_dir):
    if os.path.exists(checkpoint_dir):
        for filename in os.listdir(checkpoint_dir):
            file_path = os.path.join(checkpoint_dir, filename)
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)  # Remove file
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)  # Remove directory
            except Exception as e:
                print(f"Error deleting {file_path}: {e}")
        print(f"Contents of checkpoint directory {checkpoint_dir} removed.")
    else:
        print("Checkpoint directory does not exist.")

if __name__ == "__main__":
    schema = StructType([
        StructField("voter_id", StringType(), False),
        StructField("voter_name", StringType(), True),
        StructField("candi_id", StringType(), True),
        StructField("candi_name", StringType(), True),
        StructField("voting_time", TimestampType(), True),
        StructField("party_affiliation", StringType(), True),
        StructField("bio", StringType(), True),
        StructField("campaign_platform", StringType(), True),
        StructField("photo_url", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("registration_number", StringType(), True),
        StructField("address", MapType(StringType(), StringType()), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("cell_number", StringType(), True),
        StructField("picture", StringType(), True),
        StructField("registered_age", IntegerType(), True),
        StructField("vote", IntegerType(), True)
    ])

    voting_df = spark.readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "votes_topic") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Watermarking
    watermark_ebriched = voting_df.withWatermark("voting_time", "1 minutes")

    # Aggregations
    votres_per_candi = watermark_ebriched.groupby("candi_id", "candi_name", "party_affiliation", "photo_url") \
        .agg(sum("vote").alias("Total_Votes"))

    trunout_loc_state = watermark_ebriched.groupby(col("address").getItem("state")) \
        .agg(count("*").alias("vote_count"))

    # Write to Kafka
    p_votres_per_candi_to_kafka = votres_per_candi.select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "agg_totalvotes") \
        .option("checkpointLocation", "/workspaces/Voting-System-DE/checkpoints/cp1") \
        .outputMode('update') \
        .start()

    p_trunout_loc_state_to_kafka = trunout_loc_state.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "agg_vote_count") \
        .option("checkpointLocation", "/workspaces/Voting-System-DE/checkpoints/cp2") \
        .outputMode('update') \
        .start()

    try:
        # Start the streaming queries
        p_votres_per_candi_to_kafka.awaitTermination()
        p_trunout_loc_state_to_kafka.awaitTermination()
    except KeyboardInterrupt:
        print("Streaming terminated by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Stop the streaming queries gracefully
        print("Stopping streaming queries...")
        p_votres_per_candi_to_kafka.stop()
        p_trunout_loc_state_to_kafka.stop()

        # Clear the contents of the checkpoint directories
        print("Clearing checkpoint directories...")
        clear_checkpoint_directory("/workspaces/Voting-System-DE/checkpoints/cp1")
        clear_checkpoint_directory("/workspaces/Voting-System-DE/checkpoints/cp2")
        print("Checkpoint directories cleared.")
