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

# Global persistent DB instance
db = DatabaseManager()

processing_status = {
    "is_processing": False,
    "message": "",
    "progress": 0
}

# Utility
def extract_asin_from_url(url):
    match = re.search(r"/dp/([A-Z0-9]{10})", url)
    return match.group(1) if match else None

# Routes
@app.route("/api/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@app.route("/api/status", methods=["GET"])
def get_status():
    stats = db.get_processing_stats()
    return jsonify({"processing": processing_status, "stats": stats})

@app.route("/api/links", methods=["POST"])
def add_links():
    data = request.json
    urls = data.get("urls", [])
    product_type = data.get("product_type")

    if not urls or not product_type:
        return jsonify({"error": "URLs and product_type required"}), 400

    added = 0
    for url in urls:
        asin = extract_asin_from_url(url)
        db.insert_link(url, product_type, asin)
        added += 1

    return jsonify({"message": f"Added {added} links to '{product_type}'"}), 201

@app.route("/api/process", methods=["POST"])
def process_links():
    global processing_status
    if processing_status["is_processing"]:
        return jsonify({"error": "Processing already in progress"}), 409

    data = request.json
    skip_existing = data.get("skip_existing", True)

    def run_etl():
        global processing_status
        processing_status["is_processing"] = True
        processing_status["message"] = "Processing started..."
        try:
            pipeline = ETLPipeline()
            pipeline.run(skip_existing=skip_existing)
            processing_status["message"] = "Processing completed successfully."
        except Exception as e:
            processing_status["message"] = f"Error: {e}"
        finally:
            processing_status["is_processing"] = False

    threading.Thread(target=run_etl).start()
    return jsonify({"message": "ETL started in background"}), 202

@app.route("/api/export", methods=["GET"])
def export_reviews():
    csv_data = db.export_reviews_to_csv()
    if not csv_data:
        return jsonify({"error": "No reviews found"}), 404

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"amazon_reviews_{timestamp}.csv"
    return send_file(
        BytesIO(csv_data.encode("utf-8")),
        mimetype="text/csv",
        as_attachment=True,
        download_name=filename
    )

@app.route("/api/categories", methods=["GET"])
def get_categories():
    with db.conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT product_type
            FROM amazon_links
            WHERE product_type IS NOT NULL
            ORDER BY product_type;
        """)
        categories = [row[0] for row in cur.fetchall()]
    return jsonify({"categories": categories})

# Run app
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
