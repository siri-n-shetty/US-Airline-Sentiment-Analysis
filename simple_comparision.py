# simple_comparison.py
import psycopg2
import pandas as pd
from tabulate import tabulate
import matplotlib.pyplot as plt
import time

# Database connection
DB_CONFIG = {
    "dbname": "twitter_data",
    "user": "postgres",
    "password": "<enter your password>",
    "host": "localhost"
}

def execute_query(query):
    """Execute a query and return results as DataFrame"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        return pd.DataFrame()

def print_table(df, title):
    """Print a DataFrame as a nice table"""
    print(f"\n{title}")
    print(tabulate(df, headers='keys', tablefmt='psql', showindex=False))

def plot_comparison(df1, df2, x_col, y_col, title, filename, labels=None):
    """Create a bar chart comparing two DataFrames"""
    plt.figure(figsize=(10, 6))
    
    # Plot first dataset
    plt.bar(df1[x_col], df1[y_col], alpha=0.7, label=labels[0] if labels else "Streaming")
    
    # Plot second dataset
    plt.bar(df2[x_col], df2[y_col], alpha=0.5, label=labels[1] if labels else "Batch")
    
    plt.title(title)
    plt.xticks(rotation=45, ha='right')
    plt.legend()
    plt.tight_layout()
    plt.savefig(filename)
    print(f"Saved plot to {filename}")

def main():
    print("=== Airline Sentiment Analysis: Streaming vs. Batch Comparison ===")
    
    # 1. Compare sentiment counts
    print("\nComparing sentiment distribution...")
    streaming_sentiment = execute_query("SELECT * FROM sentiment_counts")
    batch_sentiment = execute_query("SELECT * FROM batch_sentiment_counts")
    
    print_table(streaming_sentiment, "Streaming Sentiment Counts")
    print_table(batch_sentiment, "Batch Sentiment Counts")
    # Rename for consistency before plotting
    streaming_sentiment.rename(columns={'sentiment': 'airline_sentiment'}, inplace=True)

    
    if not streaming_sentiment.empty and not batch_sentiment.empty:
        plot_comparison(
            streaming_sentiment, batch_sentiment,
            "airline_sentiment", "count",
            "Sentiment Distribution: Streaming vs. Batch",
            "sentiment_comparison.png"
        )
    
    # 2. Compare top hashtags
    print("\nComparing top hashtags...")
    streaming_hashtags = execute_query("SELECT * FROM hashtags ORDER BY count DESC LIMIT 10")
    batch_hashtags = execute_query("SELECT * FROM batch_hashtags ORDER BY count DESC LIMIT 10")
    
    print_table(streaming_hashtags, "Top Streaming Hashtags")
    print_table(batch_hashtags, "Top Batch Hashtags")
    # Rename for consistency before plotting
    streaming_hashtags.rename(columns={'word': 'hashtag'}, inplace=True)
    batch_hashtags.rename(columns={"word": "hashtag"}, inplace=True)

    
    if not streaming_hashtags.empty and not batch_hashtags.empty:
        # Use first 5 hashtags for cleaner visualization
        plot_comparison(
            streaming_hashtags.head(5), batch_hashtags.head(5),
            "hashtag", "count",
            "Top Hashtags: Streaming vs. Batch",
            "hashtag_comparison.png"
        )
    
    # 3. Compare airline sentiment
    print("\nComparing airline sentiment...")
    # For simplicity, just get negative sentiment counts by airline
    streaming_airline = execute_query("""
        SELECT airline, count FROM airline_sentiment 
        WHERE airline_sentiment = 'negative'
        ORDER BY count DESC
    """)
    
    batch_airline = execute_query("""
        SELECT airline, count FROM batch_airline_sentiment 
        WHERE airline_sentiment = 'negative'
        ORDER BY count DESC
    """)
    
    print_table(streaming_airline, "Streaming Negative Sentiment by Airline")
    print_table(batch_airline, "Batch Negative Sentiment by Airline")
    
    if not streaming_airline.empty and not batch_airline.empty:
        plot_comparison(
            streaming_airline, batch_airline,
            "airline", "count",
            "Negative Sentiment by Airline: Streaming vs. Batch",
            "airline_comparison.png"
        )
    
    print("\n=== Comparison Complete ===")
    print("Visual reports have been generated as PNG files")

if __name__ == "__main__":
    main()
