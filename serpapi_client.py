import os
import time
import requests
from bs4 import BeautifulSoup
from serpapi import GoogleSearch

class SerpAPIClient:
    def __init__(self):
        self.api_key = os.getenv("SERPAPI_KEY")

    # ---------- PRODUCT METADATA ----------
    def get_product_metadata(self, url):
        """
        Fetch metadata (title, price, rating, total reviews) from Amazon product page.
        Uses public HTML only with realistic headers — no automation.
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                print(f"[WARN] Failed to fetch {url} (Status {response.status_code})")
                return {"title": None, "price": None, "rating": None, "reviews_total": None}

            soup = BeautifulSoup(response.text, "html.parser")

            title = soup.select_one("#productTitle")
            price = soup.select_one(".a-price .a-offscreen, span.a-color-price")
            rating = soup.select_one("i[data-hook='average-star-rating'] span, span[data-hook='rating-out-of-text']")
            reviews_total = soup.select_one("#acrCustomerReviewText")

            data = {
                "title": title.get_text(strip=True) if title else None,
                "price": price.get_text(strip=True) if price else None,
                "rating": (
                    rating.get_text(strip=True).split(" ")[0] if rating else None
                ),
                "reviews_total": (
                    reviews_total.get_text(strip=True).split(" ")[0] if reviews_total else None
                ),
            }

            print(f"Parsed metadata: {data}")
            return data

        except Exception as e:
            print(f"[ERROR] Failed to extract metadata for {url}: {e}")
            return {"title": None, "price": None, "rating": None, "reviews_total": None}

    # ---------- REVIEWS (SERPAPI GOOGLE ENGINE) ----------
    def get_reviews(self, url):
        """
        Fetch review snippets using SerpApi's Google engine (free).
        """
        asin = None
        for token in ["/dp/", "/product/", "/gp/product/"]:
            if token in url:
                asin = url.split(token)[1].split("/")[0]
                break

        if not asin:
            print("[WARN] Could not extract ASIN from URL")
            return []

        params = {
            "engine": "google",
            "q": f"site:amazon.com {asin} customer reviews",
            "api_key": self.api_key,
        }

        try:
            search = GoogleSearch(params)
            result = search.get_dict()

            if "error" in result:
                print(f"[WARN] SerpApi error: {result['error']}")
                return []

            reviews = []
            for item in result.get("organic_results", []):
                snippet = item.get("snippet")
                if not snippet:
                    continue
                reviews.append({
                    "review_title": item.get("title"),
                    "review_text": snippet,
                    "rating": None,
                    "review_date": None,
                    "verified": False
                })

            print(f"Extracted {len(reviews)} reviews from Google search.")
            time.sleep(1)
            return reviews

        except Exception as e:
            print(f"[ERROR] Failed to fetch reviews via SerpApi: {e}")
            return []
