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
        Fetch metadata (title, price, average rating, total reviews)
        directly from the public Amazon product page HTML.
        We treat this as authoritative product-level info.
        """
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        }

        try:
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                print(f"[WARN] Failed to fetch {url} (Status {response.status_code})")
                return {
                    "title": None,
                    "price": None,
                    "avg_rating": None,
                    "total_reviews": None,
                }

            soup = BeautifulSoup(response.text, "html.parser")

            # Product title
            title_tag = soup.select_one("#productTitle")
            title_text = title_tag.get_text(strip=True) if title_tag else None

            # Product price
            price_tag = soup.select_one(".a-price .a-offscreen, span.a-color-price")
            price_text = price_tag.get_text(strip=True) if price_tag else None

            # Average star rating (ex: "4.5 out of 5 stars")
            avg_rating_val = None
            avg_rating_tag = soup.select_one(
                "i[data-hook='average-star-rating'] span, span[data-hook='rating-out-of-text']"
            )
            if avg_rating_tag:
                raw = avg_rating_tag.get_text(strip=True)
                first_token = raw.split(" ")[0]
                try:
                    avg_rating_val = float(first_token)
                except Exception:
                    avg_rating_val = None

            # Total number of ratings/reviews (ex: "572 ratings")
            total_reviews_val = None
            total_reviews_tag = soup.select_one("#acrCustomerReviewText")
            if total_reviews_tag:
                raw = total_reviews_tag.get_text(strip=True)
                first_token = raw.split(" ")[0].replace(",", "")
                try:
                    total_reviews_val = int(first_token)
                except Exception:
                    total_reviews_val = None

            data = {
                "title": title_text,
                "price": price_text,
                "avg_rating": avg_rating_val,
                "total_reviews": total_reviews_val,
            }

            print(f"[META] Extracted from Amazon: {data}")
            return data

        except Exception as e:
            print(f"[ERROR] Metadata extraction failed for {url}: {e}")
            return {
                "title": None,
                "price": None,
                "avg_rating": None,
                "total_reviews": None,
            }

    # ---------- INDIVIDUAL REVIEWS ----------
    def get_reviews(self, url):
        """
        Pulls individual review-like snippets from Google via SerpAPI.
        These are not complete Amazon reviews, but we treat each snippet
        as one 'review row' in amazon_reviews.

        We'll:
        - store each snippet (`review_text`)
        - try to guess a star rating from the text
        - store the snippet's title as `review_title`
        - leave review_date as None (we don't reliably have it)
        - leave verified = False (we don't reliably have it)
        """
        asin = None
        for token in ["/dp/", "/gp/product/", "/product/"]:
            if token in url:
                asin = url.split(token)[1].split("/")[0]
                break

        if not asin:
            print(f"[WARN] Could not extract ASIN from URL: {url}")
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

                # Try naive "X star" detection in snippet
                rating_guess = None
                text_lower = snippet.lower()
                for star in range(5, 0, -1):
                    # match "5 star", "5 stars", "5-star"
                    if (
                        f"{star} star" in text_lower
                        or f"{star}-star" in text_lower
                        or f"{star} stars" in text_lower
                    ):
                        rating_guess = star
                        break

                reviews.append(
                    {
                        "review_title": item.get("title"),
                        "review_text": snippet,
                        "rating": rating_guess,      # individual review rating (may be None)
                        "review_date": None,         # we can't reliably scrape this via SerpAPI Google
                        "verified": False,           # also unknown here
                    }
                )

            print(f"[REVIEWS] Extracted {len(reviews)} review snippets for {asin}.")
            # throttle so we don't hammer SerpAPI/free tier
            time.sleep(1)
            return reviews

        except Exception as e:
            print(f"[ERROR] Failed to fetch reviews via SerpApi: {e}")
            return []
