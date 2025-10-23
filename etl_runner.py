import time
from datetime import datetime
from loguru import logger
from db_manager import DatabaseManager
from serpapi_client import SerpAPIClient


class ETLPipeline:
    """
    Orchestrates the Amazon ETL workflow:
    - Reads all product links
    - Fetches metadata and reviews
    - Inserts new reviews incrementally
    """

    def __init__(self):
        self.db = DatabaseManager()
        self.client = SerpAPIClient()

    # Core ETL Logic
    def run(self, skip_existing=True):
        all_links = self.db.get_all_links()
        total = len(all_links)
        logger.info(f"Found {total} total links to scrape.")

        processed = 0
        skipped = 0
        errors = 0

        for link in all_links:
            asin = link["asin"]
            url = link["url"]

            try:
                last_extracted = self.db.get_last_extracted(asin)

                # Skip if already processed and skip_existing=True
                if skip_existing and last_extracted is not None:
                    logger.info(f"Skipping {asin} — already processed on {last_extracted}")
                    skipped += 1
                    continue

                # ------------------------------
                # 1. Fetch product metadata
                # ------------------------------
                metadata = self.client.get_product_metadata(url)
                logger.info(f"Parsed metadata: {metadata}")

                # ------------------------------
                # 2. Fetch reviews incrementally
                # ------------------------------
                all_reviews = self.client.get_reviews(url)

                new_reviews = []
                if all_reviews:
                    if last_extracted:
                        cutoff = last_extracted
                        for review in all_reviews:
                            rd = review.get("review_date")
                            # Only consider new reviews (after cutoff)
                            if rd and isinstance(rd, datetime):
                                if rd > cutoff:
                                    new_reviews.append(review)
                            else:
                                # Keep if review_date is unknown (fallback)
                                new_reviews.append(review)
                    else:
                        # First-time extraction
                        new_reviews = all_reviews

                logger.info(
                    f"Found {len(all_reviews)} reviews, inserting {len(new_reviews)} new ones for {asin}"
                )

                # ------------------------------
                # 3. Insert into DB
                # ------------------------------
                for review in new_reviews:
                    self.db.insert_review(asin, review)

                processed += 1
                logger.success(f"✓ Processed {asin}: {len(new_reviews)} new reviews inserted")

                time.sleep(2)  # polite delay to avoid SerpApi rate limits

            except Exception as e:
                errors += 1
                logger.error(f"❌ Error processing {asin}: {e}")

        logger.info("\n==================================================")
        logger.info("ETL PIPELINE SUMMARY")
        logger.info("==================================================")
        logger.info(f"Processed: {processed}")
        logger.info(f"Skipped (already exists): {skipped}")
        logger.info(f"Errors: {errors}")
        logger.info("==================================================")

        self.db.close()
        return {"processed": processed, "skipped": skipped, "errors": errors}
