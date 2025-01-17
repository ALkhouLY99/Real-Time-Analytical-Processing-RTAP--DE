import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import sum as _sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType,TimestampType



# spark set-up settings
spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.7.4") \
    .config("spark.sql.adaptive.enable", "false") \
    .getOrCreate()




if __name__ == "__main__":
    # print(pyspark.__version__)
    # print("Spark Version:", spark.version)
    # print("Application Name:", spark.sparkContext.appName)

    schema = StructType([
        StructField("voter_id",StringType(),False),
        StructField("voter_name",StringType(),True),
        StructField("candi_id",StringType(),True),
        StructField("candi_name",StringType(),True),
        StructField("voting_time",TimestampType(),True),

        StructField("party_affiliation",StringType(),True),
        StructField("bio",StringType(),True),
        StructField("campaign_platform",StringType(),True),
        StructField("photo_url",StringType(),True),


        StructField("dob",StringType(),True),
        StructField("gender",StringType(),True),
        StructField("nationality",StringType(),True),
        StructField("registration_number",StringType(),True),
        StructField("address",MapType(StringType(), StringType()),True),
        StructField("email",StringType(),True),
        StructField("phone_number",StringType(),True),
        StructField("cell_number",StringType(),True),
        StructField("picture",StringType(),True),
        StructField("registered_age",IntegerType(),True),
        StructField("vote",IntegerType(),True)
    ])

    

    voting_df = spark.readStream \
        .format('kafka')\
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "votes_topic") \
        .load()\
        .selectExpr("CAST(value AS STRING)")\
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    ## to see schema 
    # voting_df.printSchema()
    # print("voting_df variable:")
    # print(voting_df)

    # query = voting_df.writeStream\
    #     .outputMode("append")\
    #     .format("console")\
    #     .start()
    # query.awaitTermination()

    ## enricded_w_watermark
    watermark_ebriched = voting_df.withWatermark("voting_time","1 minutes")

    ## AGG_measures
    votres_per_candi = watermark_ebriched.groupby("candi_id","candi_name","party_affiliation")\
                                                .agg(_sum("vote").alias("Total_Votes"))
    trunout_by_ loc-state = watermark_ebriched.groupby(col("address").gitItem("state"))\
                                                .agg(count("*").alias("vote_count"))

    ## Write the output to the console
    # query = votres_per_candi.writeStream \
    #     .outputMode("update") \
    #     .format("console") \
    #     .start()

    # Keep the application running
    query.awaitTermination()

