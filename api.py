from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from db_manager import DatabaseManager
from etl_runner import ETLPipeline
from io import BytesIO
from datetime import datetime
import threading
import re

app = Flask(__name__)
CORS(app)

# shared DB + ETL status
db = DatabaseManager()
processing_status = {
    "is_processing": False,
    "message": "",
    "progress": 0,
}


def extract_asin_from_url(url: str):
    """
    Extract ASIN (10-char alphanumeric) from common Amazon URL forms.
    """
    m = re.search(r"/dp/([A-Z0-9]{10})", url)
    if m:
        return m.group(1)
    m = re.search(r"/gp/product/([A-Z0-9]{10})", url)
    if m:
        return m.group(1)
    return None


@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})


@app.route("/api/status", methods=["GET"])
def get_status():
    stats = db.get_processing_stats()
    return jsonify(
        {
            "processing": processing_status,
            "stats": stats,
        }
    )


@app.route("/api/links", methods=["POST"])
def add_links():
    """
    Body:
    {
      "urls": ["https://www.amazon.com/.../dp/B0XXXXX...","..."],
      "product_type": "routers"
    }
    """
    data = request.get_json(force=True)
    urls = data.get("urls", [])
    product_type = data.get("product_type")

    if not urls:
        return jsonify({"error": "No URLs provided"}), 400
    if not product_type:
        return jsonify({"error": "No product_type provided"}), 400

    added_asins = []
    for url in urls:
        asin = extract_asin_from_url(url)
        db.insert_link(asin=asin, url=url, product_type=product_type)
        added_asins.append(asin)

    return (
        jsonify(
            {
                "message": f"Successfully added {len(added_asins)} links under '{product_type}' category.",
                "asins": added_asins,
            }
        ),
        201,
    )


@app.route("/api/process", methods=["POST"])
def process_links():
    """
    Body:
    {
      "skip_existing": true  # default true
    }
    """
    global processing_status
    if processing_status["is_processing"]:
        return jsonify({"error": "Processing already in progress"}), 409

    data = request.get_json(force=True) or {}
    skip_existing = data.get("skip_existing", True)

    def run_pipeline():
        global processing_status
        processing_status["is_processing"] = True
        processing_status["message"] = "Processing started..."
        try:
            pipeline = ETLPipeline()
            pipeline.run(skip_existing=skip_existing)
            processing_status["message"] = "Processing completed successfully"
        except Exception as e:
            processing_status["message"] = f"Error: {str(e)}"
        finally:
            processing_status["is_processing"] = False

    t = threading.Thread(target=run_pipeline, daemon=True)
    t.start()

    return jsonify({"message": "Processing started"}), 202


@app.route("/api/export", methods=["GET"])
def export_reviews():
    """
    Optional query params:
    - asin=...
    - product_type=...
    """
    asin = request.args.get("asin")
    product_type = request.args.get("product_type")

    csv_data = db.export_reviews_to_csv(asin=asin, product_type=product_type)
    if not csv_data:
        return jsonify({"error": "No reviews found"}), 404

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"amazon_reviews_{ts}.csv"

    return send_file(
        BytesIO(csv_data.encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename,
    )


@app.route("/api/categories", methods=["GET"])
def get_categories():
    """
    Return all distinct product_type values from amazon_links.
    """
    with db.conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT product_type
            FROM amazon_links
            WHERE product_type IS NOT NULL
            ORDER BY product_type;
        """
        )
        rows = cur.fetchall()

    categories = [row["product_type"] for row in rows]

    return jsonify({"categories": categories})


if __name__ == "__main__":
    # dev server
    app.run(debug=True, host="0.0.0.0", port=5000)
