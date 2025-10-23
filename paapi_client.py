import os
import json
import hmac
import hashlib
import datetime
import requests

class PAAPIClient:
    def __init__(self):
        self.access_key = os.getenv("PAAPI_ACCESS_KEY")
        self.secret_key = os.getenv("PAAPI_SECRET_KEY")
        self.assoc_tag = os.getenv("PAAPI_ASSOC_TAG")
        self.host = "webservices.amazon.com"
        self.region = "us-east-1"

    def sign_request(self, payload):
        method = "POST"
        service = "ProductAdvertisingAPI"
        endpoint = f"https://{self.host}/paapi5/getitems"
        content_type = "application/json; charset=UTF-8"

        t = datetime.datetime.utcnow()
        amz_date = t.strftime("%Y%m%dT%H%M%SZ")
        datestamp = t.strftime("%Y%m%d")

        canonical_uri = "/paapi5/getitems"
        canonical_headers = f"content-type:{content_type}\nhost:{self.host}\nx-amz-date:{amz_date}\n"
        signed_headers = "content-type;host;x-amz-date"
        payload_hash = hashlib.sha256(json.dumps(payload).encode("utf-8")).hexdigest()
        canonical_request = "\n".join([method, canonical_uri, "", canonical_headers, signed_headers, payload_hash])

        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{datestamp}/{self.region}/{service}/aws4_request"
        string_to_sign = "\n".join([
            algorithm, amz_date, credential_scope,
            hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        ])

        def sign(key, msg):
            return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

        k_date = sign(("AWS4" + self.secret_key).encode("utf-8"), datestamp)
        k_region = sign(k_date, self.region)
        k_service = sign(k_region, service)
        k_signing = sign(k_service, "aws4_request")
        signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

        authorization_header = (
            f"{algorithm} Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )

        headers = {
            "Content-Type": content_type,
            "X-Amz-Date": amz_date,
            "Authorization": authorization_header
        }

        return endpoint, headers

    def get_product_info(self, asin):
        payload = {
            "ItemIds": [asin],
            "Resources": [
                "ItemInfo.Title",
                "Offers.Listings.Price",
                "CustomerReviews.Count",
                "CustomerReviews.StarRating"
            ],
            "PartnerTag": self.assoc_tag,
            "PartnerType": "Associates",
            "Marketplace": "www.amazon.com"
        }

        endpoint, headers = self.sign_request(payload)
        response = requests.post(endpoint, headers=headers, json=payload)
        try:
            item = response.json().get("ItemsResult", {}).get("Items", [])[0]
            return {
                "asin": asin,
                "title": item.get("ItemInfo", {}).get("Title", {}).get("DisplayValue"),
                "price": item.get("Offers", {}).get("Listings", [{}])[0].get("Price", {}).get("DisplayAmount"),
                "review_count": item.get("CustomerReviews", {}).get("Count"),
                "star_rating": item.get("CustomerReviews", {}).get("StarRating")
            }
        except Exception as e:
            print(f"PA-API error for {asin}: {e}")
            return {}
