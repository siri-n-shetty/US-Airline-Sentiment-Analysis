# fixed_spark_stream.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

# Create Spark Session with better configurations
spark = SparkSession.builder \
    .appName("AirlineSentimentStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .config("spark.jars", "/home/siri/Downloads/postgresql-42.2.5.jar") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.default.parallelism", "10") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

# Enable more detailed logging
spark.sparkContext.setLogLevel("WARN")

# Define schema based on your Kafka data format
schema = StructType([
    StructField("tweet_id", StringType()),
    StructField("airline_sentiment", StringType()),
    StructField("airline_sentiment_confidence", FloatType()),
    StructField("negativereason", StringType()),
    StructField("airline", StringType()),
    StructField("username", StringType()),
    StructField("tweet_text", StringType()),
    StructField("created_at", StringType()),
    StructField("retweet_count", IntegerType()),
    StructField("tweet_location", StringType()),
    StructField("user_timezone", StringType()),
    StructField("hashtags", ArrayType(StringType()))
])

# Read from Kafka topic with better error handling
try:
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "dbtech_airline_sentiment") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", 1000) \
        .load()
    
    print("Successfully connected to Kafka stream")
    
    # Parse JSON to DataFrame
    parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("processing_time", current_timestamp())
    
    # ---- HASHTAG PROCESSING ----
    # Extract and count hashtags
    hashtag_df = parsed_df.select(
        explode(col("hashtags")).alias("hashtag")
    ).filter(col("hashtag").isNotNull())
    
    hashtag_counts = hashtag_df.groupBy("hashtag").count()
    
    # Write hashtag results to console for monitoring
    query1 = hashtag_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # ---- POSTGRESQL BATCH SAVE FUNCTION ----
    def save_to_postgres(batch_df, batch_id, table_name):
        if batch_df.count() > 0:
            try:
                # Overwrite mode for hashtags as we're updating counts
                mode = "overwrite" if table_name == "hashtags" else "append"
                
                batch_df.write \
                    .format("jdbc") \
                    .option("url", "jdbc:postgresql://localhost:5432/twitter_data") \
                    .option("dbtable", table_name) \
                    .option("user", "postgres") \
                    .option("password", "SiRi123.") \
                    .option("driver", "org.postgresql.Driver") \
                    .mode(mode) \
                    .save()
                
                print(f"Successfully wrote batch {batch_id} to {table_name}")
            except Exception as e:
                print(f"Error writing to PostgreSQL: {e}")
    
    # Write hashtags to Postgres
    query2 = hashtag_counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda batch_df, batch_id: save_to_postgres(batch_df, batch_id, "hashtags")) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # ---- SENTIMENT ANALYSIS ----
    # Count by sentiment
    sentiment_counts = parsed_df.groupBy("airline_sentiment").count()
    
    # Write sentiment counts to console
    query3 = sentiment_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Write sentiment counts to Postgres
    query4 = sentiment_counts.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda batch_df, batch_id: save_to_postgres(batch_df, batch_id, "sentiment_counts")) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # ---- AIRLINE PERFORMANCE ----
    # Analyze sentiment by airline
    airline_sentiment = parsed_df.groupBy("airline", "airline_sentiment").count()
    
    # Write airline sentiment to console
    query5 = airline_sentiment.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Write airline sentiment to Postgres
    query6 = airline_sentiment.writeStream \
        .outputMode("complete") \
        .foreachBatch(lambda batch_df, batch_id: save_to_postgres(batch_df, batch_id, "airline_sentiment")) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # ---- SAVE RAW TWEETS ----
    # Write raw tweets to Postgres (limited to key fields to reduce data size)
    tweet_df = parsed_df.select(
        "tweet_id", "airline", "airline_sentiment", "tweet_text", 
        "created_at", "retweet_count", "processing_time"
    )
    
    query7 = tweet_df.writeStream \
        .outputMode("append") \
        .foreachBatch(lambda batch_df, batch_id: save_to_postgres(batch_df, batch_id, "tweets")) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    print("All streams started. Waiting for termination...")
    
    # Wait for termination
    spark.streams.awaitAnyTermination()
    
except Exception as e:
    print(f"Error in Spark streaming: {e}")
    import traceback
    traceback.print_exc()
finally:
    # Stop Spark session gracefully
    if 'spark' in locals():
        spark.stop()
        print("Spark session stopped.")