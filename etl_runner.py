from db_manager import DatabaseManager
from serpapi_client import SerpAPIClient

class ETLPipeline:
    def __init__(self):
        self.db = DatabaseManager()
        self.serpapi = SerpAPIClient()

    def extract_asin(self, url):
        for token in ["/dp/", "/product/", "/gp/product/"]:
            if token in url:
                return url.split(token)[1].split("/")[0]
        return None

    def clean_number(self, value):
        """Remove commas and non-numeric parts; return int or None."""
        if not value:
            return None
        try:
            return int(value.replace(",", "").split()[0])
        except Exception:
            return None

    def run(self):
        links = self.db.fetch_links(limit=50)
        for link in links:
            url = link["url"]
            asin = self.extract_asin(url)
            if not asin:
                print(f"Could not extract ASIN from {url}")
                continue

            # ---- SERPAPI + HTML ----
            try:
                product_data = self.serpapi.get_product_metadata(url)
                reviews = self.serpapi.get_reviews(url)
            except Exception as e:
                print(f"Error fetching data for {asin}: {e}")
                continue

            total_reviews = self.clean_number(product_data.get("reviews_total"))
            avg_star_rating = None
            try:
                avg_star_rating = float(product_data.get("rating")) if product_data.get("rating") else None
            except ValueError:
                avg_star_rating = None

            for r in reviews:
                review_record = {
                    "asin": asin,
                    "product_url": url,
                    "title": product_data.get("title"),
                    "price": product_data.get("price"),
                    "avg_star_rating": avg_star_rating,
                    "total_reviews": total_reviews,
                    "review_title": r.get("review_title"),
                    "review_text": r.get("review_text"),
                    "rating": r.get("rating"),
                    "review_date": r.get("review_date"),
                    "verified": r.get("verified"),
                }
                self.db.insert_review(review_record)

        self.db.close()
