import psycopg
from psycopg.rows import dict_row
from loguru import logger
import os
from dotenv import load_dotenv
import csv
from io import StringIO

class DatabaseManager:
    """Manages all PostgreSQL operations for the Amazon Reviews ETL pipeline."""

    _shared_conn = None  # persistent connection reused across API and ETL

    def __init__(self):
        # Load environment variables
        load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

        # Reuse connection if already established
        if DatabaseManager._shared_conn:
            self.conn = DatabaseManager._shared_conn
            return

        # Establish a single shared connection
        DatabaseManager._shared_conn = psycopg.connect(
            host=os.getenv("PG_HOST"),
            dbname=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT"),
            sslmode=os.getenv("PG_SSLMODE", "require"),
            sslrootcert=os.getenv("PG_SSLROOTCERT", "ca.pem"),
            row_factory=dict_row
        )
        DatabaseManager._shared_conn.autocommit = True
        self.conn = DatabaseManager._shared_conn
        logger.success(f"Connected to PostgreSQL at {os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}")
        self.ensure_tables_exist()

    # Table setup
    def ensure_tables_exist(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS amazon_links (
                    id SERIAL PRIMARY KEY,
                    url TEXT UNIQUE,
                    asin TEXT,
                    product_type TEXT,
                    product_name TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS amazon_reviews (
                    id SERIAL PRIMARY KEY,
                    asin TEXT,
                    product_url TEXT,
                    product_name TEXT,
                    review_title TEXT,
                    review_text TEXT,
                    rating TEXT,
                    reviewer_name TEXT,
                    review_date TEXT,
                    verified BOOLEAN DEFAULT FALSE,
                    inserted_at TIMESTAMP DEFAULT NOW()
                );
            """)
        logger.info("Verified all tables exist.")

    # Utility
    def get_all_links(self):
        """Return all URLs from amazon_links."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM amazon_links ORDER BY id;")
            return cur.fetchall()

    def get_processing_stats(self):
        """Return total link and review counts."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS total_links FROM amazon_links;")
            total_links = cur.fetchone()["total_links"]

            cur.execute("SELECT COUNT(*) AS total_reviews FROM amazon_reviews;")
            total_reviews = cur.fetchone()["total_reviews"]

        return {"total_links": total_links, "total_reviews": total_reviews}

    # URL and review checks
    def url_in_links(self, url):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM amazon_links WHERE url = %s;", (url,))
            return cur.fetchone() is not None

    def url_in_reviews(self, url):
        with self.conn.cursor() as cur:
            cur.execute("SELECT 1 FROM amazon_reviews WHERE product_url = %s;", (url,))
            return cur.fetchone() is not None

    def url_has_reviews(self, url):
        """Alias for compatibility."""
        return self.url_in_reviews(url)

    # Insert & Update
    def insert_link(self, url, product_type, asin=None, product_name=None):
        """Insert link if not already present."""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO amazon_links (url, product_type, asin, product_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING;
            """, (url, product_type, asin, product_name))
        logger.info(f"Added/verified link: {url}")

    def insert_review_by_url(self, url, review_data):
        """Insert a review entry for a given product URL."""
        if not url or not review_data.get("review_text"):
            return

        with self.conn.cursor() as cur:
            cur.execute("SELECT asin, product_name FROM amazon_links WHERE url = %s;", (url,))
            link = cur.fetchone()
            asin = link["asin"] if link else None
            product_name = link["product_name"] if link else None

            cur.execute("""
                SELECT 1 FROM amazon_reviews
                WHERE review_text = %s AND product_url = %s;
            """, (review_data["review_text"], url))
            if cur.fetchone():
                return

            cur.execute("""
                INSERT INTO amazon_reviews (
                    asin, product_url, product_name,
                    review_title, review_text, rating,
                    reviewer_name, review_date, verified
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                asin, url, product_name,
                review_data.get("review_title"),
                review_data.get("review_text"),
                review_data.get("rating"),
                review_data.get("reviewer_name"),
                review_data.get("review_date"),
                review_data.get("verified", False),
            ))
        logger.debug(f"Inserted review for {url}")

    def update_product_name_by_url(self, url, product_name):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE amazon_links
                SET product_name = %s
                WHERE url = %s;
            """, (product_name, url))
        logger.info(f"Updated product name for {url}")

    # Data cleanup
    def clear_all_reviews(self):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM amazon_reviews;")
        logger.warning("Cleared all records from amazon_reviews.")

    # CSV Export
    def export_reviews_to_csv(self, asin=None, product_type=None):
        with self.conn.cursor() as cur:
            query = """
                SELECT asin, product_url, product_name, review_title,
                       review_text, rating, reviewer_name, review_date,
                       verified, inserted_at
                FROM amazon_reviews
            """
            cur.execute(query)
            rows = cur.fetchall()

        if not rows:
            return None

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()
