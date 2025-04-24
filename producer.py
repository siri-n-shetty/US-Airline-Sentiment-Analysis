# kafka_producer.py
import pandas as pd
from kafka import KafkaProducer
import json
import time

# Load CSV
df = pd.read_csv('Tweets.csv')  # Ensure the file is in the working directory

# Clean and convert rows to dictionaries
def preprocess(row):
    hashtags = [word[1:] for word in row['text'].split() if word.startswith("#")]
    return {
        "tweet_id": str(row["tweet_id"]),
        "airline_sentiment": row["airline_sentiment"],
        "airline_sentiment_confidence": float(row["airline_sentiment_confidence"]),
        "negativereason": row.get("negativereason", None),
        "airline": row["airline"],
        "username": row["name"],
        "tweet_text": row["text"],
        "created_at": row["tweet_created"],
        "retweet_count": int(row["retweet_count"]),
        "tweet_location": row.get("tweet_location", None),
        "user_timezone": row.get("user_timezone", None),
        "hashtags": hashtags
    }

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Send tweets one by one to Kafka
for _, row in df.iterrows():
    tweet_json = preprocess(row)
    producer.send('dbtech_airline_sentiment', tweet_json)
    print(f"Sent tweet ID {tweet_json['tweet_id']}")
    time.sleep(0.1)  # Slow down to simulate streaming

producer.flush()
producer.close()