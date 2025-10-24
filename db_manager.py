import os
import csv
from io import StringIO
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from loguru import logger


class DatabaseManager:
    """
    Handles all database operations for the Amazon Reviews ETL pipeline.
    We keep a single shared connection so the API and ETL can both reuse it.
    """

    _shared_conn = None

    def __init__(self):
        # Load .env (local dev)
        load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))

        if DatabaseManager._shared_conn:
            self.conn = DatabaseManager._shared_conn
        else:
            DatabaseManager._shared_conn = psycopg.connect(
                host=os.getenv("PG_HOST"),
                dbname=os.getenv("PG_DATABASE"),
                user=os.getenv("PG_USER"),
                password=os.getenv("PG_PASSWORD"),
                port=os.getenv("PG_PORT"),
                sslmode=os.getenv("PG_SSLMODE", "require"),
                sslrootcert=os.getenv("PG_SSLROOTCERT", "ca.pem"),
                row_factory=dict_row,
            )
            DatabaseManager._shared_conn.autocommit = True
            self.conn = DatabaseManager._shared_conn
            logger.success(
                f"Connected to PostgreSQL at {os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}"
            )

        self.ensure_tables_exist()

    # ---------- SCHEMA / MIGRATION ----------
    def ensure_tables_exist(self):
        """
        Make sure our two core tables exist and have the final columns we care about.
        - amazon_links: one row per product URL
        - amazon_reviews: many rows per product URL (snippets)
        """
        with self.conn.cursor() as cur:
            # products / URLs
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS amazon_links (
                    id SERIAL PRIMARY KEY,
                    asin TEXT,
                    url TEXT UNIQUE,
                    product_type TEXT,
                    product_name TEXT,
                    added_on TIMESTAMP DEFAULT NOW()
                );
            """
            )

            # individual review snippets + product metadata snapshot
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS amazon_reviews (
                    id SERIAL PRIMARY KEY,
                    asin TEXT,
                    product_url TEXT,
                    product_name TEXT,
                    price TEXT,
                    avg_star_rating FLOAT,
                    review_title TEXT,
                    review_text TEXT,
                    rating FLOAT,
                    review_date TEXT,
                    verified BOOLEAN DEFAULT FALSE,
                    inserted_on TIMESTAMP DEFAULT NOW()
                );
            """
            )

        logger.info("Verified / created tables.")

    # ---------- LINK MGMT ----------
    def insert_link(self, asin, url, product_type):
        """
        Insert new product into amazon_links or update existing.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO amazon_links (asin, url, product_type)
                VALUES (%s, %s, %s)
                ON CONFLICT (url)
                DO UPDATE SET asin = EXCLUDED.asin,
                              product_type = EXCLUDED.product_type;
            """,
                (asin, url, product_type),
            )

    def get_all_links(self):
        """
        Return all product links we know about.
        """
        with self.conn.cursor() as cur:
            cur.execute("SELECT * FROM amazon_links ORDER BY id;")
            return cur.fetchall()

    def update_product_metadata_for_url(self, url, product_name, price, avg_star_rating):
        """
        Store product_name in amazon_links for display/export downstream.
        (Price and avg_star_rating we repeat per review row instead of here,
        because they can fluctuate over time.)
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE amazon_links
                SET product_name = %s
                WHERE url = %s;
            """,
                (product_name, url),
            )
        logger.info(f"Updated product name for {url}")

        # we don't store price / avg in amazon_links for now,
        # we snapshot them with each review row

    # ---------- REVIEW INSERTION ----------
    def insert_review_row(
        self,
        asin,
        product_url,
        product_name,
        price,
        avg_star_rating,
        review_title,
        review_text,
        rating,
        review_date,
        verified=False,
    ):
        """
        Insert one review row into amazon_reviews unless it's already present.
        Identity check = same product_url + same review_text.
        """
        if not product_url or not review_text:
            return

        with self.conn.cursor() as cur:
            # dedupe
            cur.execute(
                """
                SELECT 1 FROM amazon_reviews
                WHERE product_url = %s AND review_text = %s;
            """,
                (product_url, review_text),
            )
            if cur.fetchone():
                return  # already stored

            cur.execute(
                """
                INSERT INTO amazon_reviews (
                    asin,
                    product_url,
                    product_name,
                    price,
                    avg_star_rating,
                    review_title,
                    review_text,
                    rating,
                    review_date,
                    verified
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
            """,
                (
                    asin,
                    product_url,
                    product_name,
                    price,
                    avg_star_rating,
                    review_title,
                    review_text,
                    rating,
                    review_date,
                    verified,
                ),
            )

    # ---------- DASHBOARD STATS ----------
    def get_processing_stats(self):
        """
        Dashboard summary numbers.
        """
        with self.conn.cursor() as cur:
            # total links
            cur.execute("SELECT COUNT(*) AS total_links FROM amazon_links;")
            total_links = cur.fetchone()["total_links"]

            # how many unique asins actually have at least one review row
            cur.execute(
                """
                SELECT COUNT(DISTINCT asin) AS processed_asins
                FROM amazon_reviews
                WHERE asin IS NOT NULL;
            """
            )
            processed_asins = cur.fetchone()["processed_asins"]

            # links with zero reviews
            cur.execute(
                """
                SELECT COUNT(*) AS pending_asins
                FROM amazon_links l
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM amazon_reviews r
                    WHERE r.product_url = l.url
                );
            """
            )
            pending_asins = cur.fetchone()["pending_asins"]

            # total number of review rows
            cur.execute("SELECT COUNT(*) AS total_reviews FROM amazon_reviews;")
            total_reviews = cur.fetchone()["total_reviews"]

        return {
            "total_links": total_links,
            "processed_asins": processed_asins,
            "pending_asins": pending_asins,
            "total_reviews": total_reviews,
        }

    # ---------- MAINTENANCE ----------
    def clear_all_reviews(self):
        """
        Danger zone: wipe all review rows.
        We call this if skip_existing=False to "full refresh".
        """
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM amazon_reviews;")
        logger.warning("Cleared all records from amazon_reviews.")

    # ---------- EXPORT ----------
    def export_reviews_to_csv(self, asin=None, product_type=None):
        """
        Dump the review rows as CSV.
        We can support filtering by asin/product_type later.
        """
        with self.conn.cursor() as cur:
            base_query = """
                SELECT
                    asin,
                    product_url,
                    product_name,
                    price,
                    avg_star_rating,
                    review_title,
                    review_text,
                    rating,
                    review_date,
                    verified,
                    inserted_on
                FROM amazon_reviews
            """

            filters = []
            params = []

            if asin:
                filters.append("asin = %s")
                params.append(asin)

            if product_type:
                # join to links to filter by category
                base_query += " JOIN amazon_links al ON amazon_reviews.product_url = al.url "
                filters.append("al.product_type = %s")
                params.append(product_type)

            if filters:
                base_query += " WHERE " + " AND ".join(filters)

            base_query += " ORDER BY inserted_on DESC;"

            cur.execute(base_query, params)
            rows = cur.fetchall()

        if not rows:
            return None

        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
        return output.getvalue()
