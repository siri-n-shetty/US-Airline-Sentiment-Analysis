import psycopg2

# 1. Connect to PostgreSQL
conn = psycopg2.connect(
    dbname="twitter_data",
    user="postgres",
    password="SiRi123.",
    host="localhost",
    port="5432"
)

cur = conn.cursor()

# 2. Insert hardcoded hashtag data
hashtags = [
    ("#United", 120),
    ("#Delayed", 85),
    ("#NeverAgain", 60),
    ("#Delta", 99),
    ("#flightdelay", 44)
]

for tag, count in hashtags:
    try:
        cur.execute(
            "INSERT INTO hashtags (tag, count) VALUES (%s, %s) ON CONFLICT (tag) DO UPDATE SET count = EXCLUDED.count",
            (tag, count)
        )
    except Exception as e:
        print(f"Error inserting {tag}: {e}")

# 3. Commit & close
conn.commit()
cur.close()
conn.close()

print("✅ Inserted hashtags into DB.")
