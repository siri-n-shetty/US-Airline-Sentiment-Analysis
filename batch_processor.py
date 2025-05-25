# simplified_batch_processor.py
import time
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, regexp_extract

# Create Spark session with better configurations
spark = SparkSession.builder \
    .appName("AirlineSentimentBatch") \
    .config("spark.jars", "/home/siri/Downloads/postgresql-42.2.5.jar") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "10") \
    .getOrCreate()

# Set higher log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

# Timing function
def time_execution(func):
    start = time.time()
    result = func()
    end = time.time()
    return result, end - start

try:
    print("Starting batch processing...")
    batch_start = time.time()
    
    # 1. Load the dataset
    print("Loading CSV dataset...")
    
    tweets_df = spark.read.csv('Tweets.csv', header=True, inferSchema=True)
    tweets_count = tweets_df.count()
    print(f"Loaded {tweets_count} tweets from CSV.")
    
    # Register as temp view for SQL
    tweets_df.createOrReplaceTempView("tweets")
    
    # 2. Extract hashtags from tweet text
    print("Extracting hashtags...")
    tweets_df = tweets_df.withColumn(
        "hashtags", 
        split(regexp_extract(col("text"), r"(#\w+)", 1), " ")
    )
    
    # 3. Analyze sentiment counts
    print("Analyzing sentiment distribution...")
    sentiment_counts = spark.sql("""
        SELECT airline_sentiment, COUNT(*) as count
        FROM tweets
        GROUP BY airline_sentiment
        ORDER BY count DESC
    """)
    
    print("Sentiment distribution:")
    sentiment_counts.show()
    
    # 4. Analyze hashtags
    print("Analyzing hashtags...")
    hashtags_df = tweets_df.select(
        explode(split(col("text"), " ")).alias("word")
    ).filter(col("word").startswith("#"))
    
    hashtag_counts = hashtags_df.groupBy("word").count().orderBy(col("count").desc())
    
    print("Top hashtags:")
    hashtag_counts.show(10)
    
    # 5. Analyze airline performance
    print("Analyzing airline performance...")
    airline_sentiment = spark.sql("""
        SELECT airline, airline_sentiment, COUNT(*) as count
        FROM tweets
        GROUP BY airline, airline_sentiment
        ORDER BY airline, count DESC
    """)
    
    print("Airline sentiment distribution:")
    airline_sentiment.show()
    
    # 6. Store results in PostgreSQL
    print("Storing results in PostgreSQL...")
    
    # Store sentiment counts
    sentiment_counts.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/twitter_data") \
        .option("dbtable", "batch_sentiment_counts") \
        .option("user", "postgres") \
        .option("password", "<enter your password>") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    # Store hashtag counts
    hashtag_counts.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/twitter_data") \
        .option("dbtable", "batch_hashtags") \
        .option("user", "postgres") \
        .option("password", "<enter your password>") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    # Store airline sentiment
    airline_sentiment.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/twitter_data") \
        .option("dbtable", "batch_airline_sentiment") \
        .option("user", "postgres") \
        .option("password", "<enter your password>") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    batch_end = time.time()
    total_time = batch_end - batch_start
    
    print(f"Batch processing completed in {total_time:.2f} seconds.")
    
    # 7. Save performance metrics
    performance = {
        "processing_type": "batch",
        "records_processed": tweets_count,
        "processing_time_seconds": total_time,
    }
    
    # Convert to DataFrame and save
    perf_df = spark.createDataFrame([performance])
    perf_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/twitter_data") \
        .option("dbtable", "performance_metrics") \
        .option("user", "postgres") \
        .option("password", "<enter your password>") \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()
    
except Exception as e:
    print(f"Error in batch processing: {e}")
    import traceback
    traceback.print_exc()
finally:
    # Stop Spark session gracefully
    if 'spark' in locals():
        spark.stop()
        print("Spark session stopped.")
