# US-Airline-Sentiment-Analysis

This project implements a data analytics pipeline that compares real-time streaming and offline batch processing using the [Twitter US Airline Sentiment](https://www.kaggle.com/datasets/crowdflower/twitter-airline-sentiment) dataset. Leveraging tools such as **Apache Kafka**, **Apache Spark**, and **PostgreSQL**, I captured and processed tweet data to analyze sentiment distribution, hashtag frequency, and airline-specific sentiment trends.

This was done as a Mini project for the course **Database Technologies (UE22CS343BB3)**.

The streaming component ingested tweet data through Kafka and processed it using Spark Structured Streaming. In parallel, the batch mode processed the same dataset offline using Spark and Pandas for comparison. Both modes stored their outputs in PostgreSQL, enabling visual and statistical comparisons. 

## System Requirements

- Python >= 3.8+
- Apache Kafka and Zookeeper
- Apache Spark
- PostgreSQL installed and running
- Java 8+
- Python libraries (pandas, time, pyspark.sql, kafka, json, psycopg2, tabulate, matplotlib)

## General Steps to Run 

- Start Zookeeper and Kafka
- Create Kafka topic
- Start PostgreSQL
- Create DB
- Load Tweets into Kafka
- Perform Real-Time Analysis (Spark Streaming)
- Perform Batch Analysis
- Comparison Script (Optional)

***Notes***: Project was developed and tested in WSL2 (Ubuntu) but instructions are platform-agnostic. You may adapt DB credentials and file paths per your OS.

Feel free to fork, extend, and use this for streaming & data engineering demos!
