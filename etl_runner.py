import time
from loguru import logger
from db_manager import DatabaseManager
from serpapi_client import SerpAPIClient


class ETLPipeline:
    """
    Orchestrates:
    1. load all product links from DB
    2. for each link:
       - get product metadata (price, avg stars, etc.)
       - get review snippets
       - insert rows into amazon_reviews
    """

    def __init__(self):
        self.db = DatabaseManager()
        self.client = SerpAPIClient()

    def run(self, skip_existing=True):
        links = self.db.get_all_links()
        total = len(links)
        logger.info(f"Found {total} total product links.")

        if not skip_existing:
            logger.warning("Reprocessing all — clearing amazon_reviews table.")
            self.db.clear_all_reviews()
            time.sleep(1)

        processed = 0
        skipped = 0
        errors = 0

        for link in links:
            url = link["url"]
            asin = link["asin"]

            try:
                # if skipping and we already stored reviews for this URL, skip
                if skip_existing:
                    # already have at least one review row for this URL?
                    with self.db.conn.cursor() as cur:
                        cur.execute(
                            "SELECT 1 FROM amazon_reviews WHERE product_url = %s LIMIT 1;",
                            (url,),
                        )
                        if cur.fetchone():
                            logger.info(f"Skipping {url} (already have reviews).")
                            skipped += 1
                            continue

                # --- METADATA ---
                meta = self.client.get_product_metadata(url)
                product_name = meta.get("title")
                price = meta.get("price")
                avg_star_rating = meta.get("avg_rating")

                # store product_name for display in /api/status, etc.
                if product_name:
                    self.db.update_product_metadata_for_url(
                        url,
                        product_name=product_name,
                        price=price,
                        avg_star_rating=avg_star_rating,
                    )

                logger.info(f"Metadata for {url}: {meta}")

                # --- REVIEWS ---
                review_snippets = self.client.get_reviews(url)
                if not review_snippets:
                    logger.warning(f"No reviews found for {url}")
                    continue

                for r in review_snippets:
                    self.db.insert_review_row(
                        asin=asin,
                        product_url=url,
                        product_name=product_name,
                        price=price,
                        avg_star_rating=avg_star_rating,
                        review_title=r.get("review_title"),
                        review_text=r.get("review_text"),
                        rating=r.get("rating"),
                        review_date=r.get("review_date"),
                        verified=r.get("verified", False),
                    )

                processed += 1
                logger.success(
                    f"Processed {url}: inserted {len(review_snippets)} review rows"
                )
                time.sleep(1.5)

            except Exception as e:
                errors += 1
                logger.error(f"Error processing {url}: {e}")

        logger.info("========== ETL SUMMARY ==========")
        logger.info(f"Processed: {processed}")
        logger.info(f"Skipped:   {skipped}")
        logger.info(f"Errors:    {errors}")
        logger.info("=================================")

        return {"processed": processed, "skipped": skipped, "errors": errors}
