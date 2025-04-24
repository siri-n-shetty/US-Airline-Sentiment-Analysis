# kafka_consumer.py
from kafka import KafkaConsumer
import json

# Create Kafka consumer
consumer = KafkaConsumer(
    'dbtech_airline_sentiment',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='tweet_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Listening to Kafka topic: dbtech_airline_sentiment\n")

# Consume tweets from Kafka
for message in consumer:
    tweet = message.value

    print(f"""
 Tweet ID: {tweet['tweet_id']}
 Airline: {tweet['airline']}
 Sentiment: {tweet['airline_sentiment']} (Confidence: {tweet['airline_sentiment_confidence']})
 Text: {tweet['tweet_text']}
 Created at: {tweet['created_at']}
 Retweets: {tweet['retweet_count']}
 Location: {tweet['tweet_location'] or 'N/A'}
 Timezone: {tweet['user_timezone'] or 'N/A'}
 Hashtags: {', '.join(tweet['hashtags']) if tweet['hashtags'] else 'None'}
 Reason (if negative): {tweet['negativereason'] or 'N/A'}
    """)
