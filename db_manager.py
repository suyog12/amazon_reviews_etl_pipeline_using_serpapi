import psycopg
from psycopg.rows import dict_row
from datetime import datetime
from loguru import logger
import os
from dotenv import load_dotenv


class DatabaseManager:
    """
    Handles all database operations for the Amazon Reviews ETL pipeline.
    """

    def __init__(self):
        # Load .env from current directory
        load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

        self.conn = psycopg.connect(
            host=os.getenv("PG_HOST"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT"),
            sslmode=os.getenv("PG_SSLMODE", "require"),
            sslrootcert=os.getenv("PG_SSLROOTCERT", "ca.pem"),
            row_factory=dict_row
        )
        self.conn.autocommit = True
        logger.success(f"Connected to PostgreSQL at {os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}")
        self.ensure_tables_exist()

    # Table Setup
    def ensure_tables_exist(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS amazon_links (
                    id SERIAL PRIMARY KEY,
                    asin TEXT UNIQUE,
                    url TEXT,
                    product_type TEXT,
                    added_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS amazon_reviews (
                    id SERIAL PRIMARY KEY,
                    asin TEXT,
                    review_title TEXT,
                    review_text TEXT,
                    rating FLOAT,
                    reviewer_name TEXT,
                    review_date TIMESTAMP,
                    inserted_on TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS etl_log (
                    asin TEXT PRIMARY KEY,
                    last_extracted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        logger.info("Verified or created all required tables.")

    # Link Management
    def insert_link(self, asin, url, product_type):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO amazon_links (asin, url, product_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (asin)
                DO UPDATE SET url = EXCLUDED.url, product_type = EXCLUDED.product_type;
            """, (asin, url, product_type))

    def get_all_links(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM amazon_links ORDER BY id;")
            return cur.fetchall()

    # Smart Review Insertion (Only New Reviews)
    def insert_review(self, asin, review_data):
        """
        Insert only new reviews for a given ASIN.
        """
        if not asin or not review_data.get("review_text"):
            return

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM amazon_reviews
                WHERE asin = %s AND review_text = %s;
            """, (asin, review_data.get("review_text")))
            if cur.fetchone():
                return  # Already exists

            cur.execute("""
                INSERT INTO amazon_reviews (
                    asin, review_title, review_text, rating, reviewer_name, review_date
                )
                VALUES (%s, %s, %s, %s, %s, %s);
            """, (
                asin,
                review_data.get("review_title"),
                review_data.get("review_text"),
                review_data.get("rating"),
                review_data.get("reviewer_name"),
                review_data.get("review_date"),
            ))

        self.update_last_extracted(asin)

    # Incremental ETL Tracking
    def get_last_extracted(self, asin):
        with self.conn.cursor() as cur:
            cur.execute("SELECT last_extracted FROM etl_log WHERE asin = %s;", (asin,))
            row = cur.fetchone()
            return row["last_extracted"] if row else None

    def update_last_extracted(self, asin):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO etl_log (asin, last_extracted)
                VALUES (%s, %s)
                ON CONFLICT (asin) DO UPDATE SET last_extracted = EXCLUDED.last_extracted;
            """, (asin, datetime.utcnow()))

    # Dashboard Stats
    def count_total_links(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM amazon_links;")
            return cur.fetchone()["total"]

    def count_processed_links(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT asin) AS total FROM amazon_reviews;")
            return cur.fetchone()["total"]

    def count_pending_links(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total
                FROM amazon_links l
                WHERE NOT EXISTS (
                    SELECT 1 FROM amazon_reviews r WHERE r.asin = l.asin
                );
            """)
            return cur.fetchone()["total"]

    def count_total_reviews(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total FROM amazon_reviews;")
            return cur.fetchone()["total"]
    
    def count_no_reviews_links(self):
        """Count URLs that exist in amazon_links but not in amazon_reviews."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS total
                FROM amazon_links al
                LEFT JOIN amazon_reviews ar
                ON al.url = ar.product_url
                WHERE ar.product_url IS NULL;
            """)
            return cur.fetchone()["total"]
        
    def get_processing_stats(self):
        stats = {
            "total_links": self.count_total_links(),
            "processed_asins": self.count_processed_links(),  # match frontend
            "pending_asins": self.count_pending_links(),
            "total_reviews": self.count_total_reviews(),
            "no_reviews_found": self.count_no_reviews_links()
        }
        return stats

    def close(self):
        self.conn.close()
