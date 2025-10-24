import time
from loguru import logger
from db_manager import DatabaseManager
from serpapi_client import SerpAPIClient

class ETLPipeline:
    """Manages ETL logic for Amazon review extraction and storage."""

    def __init__(self):
        self.db = DatabaseManager()
        self.client = SerpAPIClient()

    def run(self, skip_existing=True):
        all_links = self.db.get_all_links()
        total = len(all_links)
        logger.info(f"Found {total} total product links.")

        # Reprocess All
        if not skip_existing:
            logger.warning("Reprocessing all — clearing amazon_reviews table.")
            self.db.clear_all_reviews()
            time.sleep(1)

        processed, skipped, errors = 0, 0, 0

        for link in all_links:
            url = link["url"]

            try:
                if skip_existing and self.db.url_in_reviews(url):
                    logger.info(f"Skipping {url} (already reviewed).")
                    skipped += 1
                    continue

                # Fetch metadata
                metadata = self.client.get_product_metadata(url)
                if metadata.get("title"):
                    self.db.update_product_name_by_url(url, metadata["title"])
                logger.info(f"Metadata: {metadata}")

                # Fetch reviews
                reviews = self.client.get_reviews(url)
                if not reviews:
                    logger.warning(f"No reviews found for {url}")
                    continue

                for review in reviews:
                    self.db.insert_review_by_url(url, {
                        "review_title": review.get("review_title"),
                        "review_text": review.get("review_text"),
                        "rating": review.get("rating"),
                        "reviewer_name": review.get("reviewer_name"),
                        "review_date": review.get("review_date"),
                    })

                processed += 1
                logger.success(f"Processed {url}: {len(reviews)} reviews inserted")
                time.sleep(2)

            except Exception as e:
                errors += 1
                logger.error(f"Error processing {url}: {e}")

        logger.info("\n========== ETL SUMMARY ==========")
        logger.info(f"Processed: {processed}")
        logger.info(f"Skipped: {skipped}")
        logger.info(f"Errors: {errors}")
        logger.info("=================================")
        return {"processed": processed, "skipped": skipped, "errors": errors}
