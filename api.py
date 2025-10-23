from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from db_manager import DatabaseManager
from etl_runner import ETLPipeline
import threading
from io import BytesIO
from datetime import datetime
import re

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

# Store processing status
processing_status = {
    "is_processing": False,
    "message": "",
    "progress": 0
}

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

def extract_asin_from_url(url):
    """Extract ASIN (Amazon product ID) from an Amazon URL."""
    match = re.search(r"/dp/([A-Z0-9]{10})", url)
    return match.group(1) if match else None

@app.route('/api/links', methods=['POST'])
def add_links():
    """Add new Amazon links to be processed"""
    data = request.json
    urls = data.get('urls', [])
    product_type = data.get('product_type')

    if not urls:
        return jsonify({"error": "No URLs provided"}), 400
    if not product_type:
        return jsonify({"error": "No product_type provided"}), 400

    db = DatabaseManager()
    added_asins = []

    try:
        for url in urls:
            asin = extract_asin_from_url(url)
            if not asin:
                continue
            db.insert_link(asin, url, product_type)
            added_asins.append(asin)

        db.close()
        return jsonify({
            "message": f"Successfully added {len(added_asins)} links under '{product_type}' category.",
            "asins": added_asins
        }), 201

    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/process', methods=['POST'])
def process_links():
    """Trigger ETL processing"""
    global processing_status
    
    if processing_status["is_processing"]:
        return jsonify({"error": "Processing already in progress"}), 409
    
    data = request.json
    skip_existing = data.get('skip_existing', True)
    
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
    
    # Run in background thread
    thread = threading.Thread(target=run_pipeline)
    thread.start()
    
    return jsonify({"message": "Processing started in background"}), 202

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current processing status"""
    db = DatabaseManager()
    stats = db.get_processing_stats()
    db.close()
    
    return jsonify({
        "processing": processing_status,
        "stats": stats
    })

@app.route('/api/export', methods=['GET'])
def export_reviews():
    """Export reviews as CSV"""
    asin = request.args.get('asin')
    product_type = request.args.get('product_type')
    
    db = DatabaseManager()
    
    try:
        csv_data = db.export_reviews_to_csv(asin=asin, product_type=product_type)
        
        if not csv_data:
            db.close()
            return jsonify({"error": "No reviews found"}), 404
        
        db.close()
        
        # Create filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"amazon_reviews_{timestamp}.csv"
        
        # Send as downloadable file
        return send_file(
            BytesIO(csv_data.encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
    
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/reviews', methods=['GET'])
def get_reviews():
    """Get reviews as JSON (for preview)"""
    asin = request.args.get('asin')
    product_type = request.args.get('product_type')
    limit = request.args.get('limit', 100, type=int)
    
    db = DatabaseManager()
    
    try:
        with db.conn.cursor() as cur:
            query = """
                SELECT 
                    ar.asin,
                    ar.title AS product_title,
                    ar.price,
                    ar.avg_star_rating,
                    ar.total_reviews,
                    ar.review_title,
                    ar.review_text,
                    ar.rating,
                    ar.review_date,
                    ar.verified
                FROM amazon_reviews ar
            """
            
            conditions = []
            params = []
            
            if asin:
                conditions.append("ar.asin = %s")
                params.append(asin)
            
            if product_type:
                query += " LEFT JOIN amazon_links al ON ar.product_url = al.url"
                conditions.append("al.product_type = %s")
                params.append(product_type)
            
            if conditions:
                query += " WHERE " + " AND ".join(conditions)
            
            query += f" ORDER BY ar.created_at DESC LIMIT {limit};"
            
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            reviews = [dict(zip(columns, row)) for row in rows]
        
        db.close()
        return jsonify({"reviews": reviews, "count": len(reviews)})
    
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get list of all product categories"""
    db = DatabaseManager()
    
    try:
        with db.conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT product_type 
                FROM amazon_links 
                WHERE product_type IS NOT NULL
                ORDER BY product_type;
            """)
            categories = [row[0] for row in cur.fetchall()]
        
        db.close()
        return jsonify({"categories": categories})
    
    except Exception as e:
        db.close()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)