import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            port=os.getenv("PG_PORT"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            sslmode=os.getenv("PG_SSLMODE"),
            sslrootcert=os.getenv("PG_SSLROOTCERT"),
        )
        self.conn.autocommit = True

    def fetch_links(self, limit=50):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, url, product_type FROM amazon_links LIMIT %s;", (limit,))
            return cur.fetchall()

    def insert_review(self, review_data):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO amazon_reviews (
                    asin, product_url, title, price, avg_star_rating, total_reviews,
                    review_title, review_text, rating, review_date, verified
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """, (
                review_data.get("asin"),
                review_data.get("product_url"),
                review_data.get("title"),
                review_data.get("price"),
                review_data.get("avg_star_rating"),
                review_data.get("total_reviews"),
                review_data.get("review_title"),
                review_data.get("review_text"),
                review_data.get("rating"),
                review_data.get("review_date"),
                review_data.get("verified")
            ))

    def close(self):
        self.conn.close()
