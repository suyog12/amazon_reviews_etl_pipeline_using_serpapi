# Amazon Reviews ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline for collecting Amazon product metadata and customer reviews. This system features a Flask API backend, PostgreSQL database, and a web-based dashboard for managing the entire data collection workflow.

## Features

- **Web-Based Dashboard**: Interactive HTML interface for managing URLs, monitoring progress, and exporting data
- **Flask REST API**: Complete backend API for all ETL operations
- **Product Metadata Extraction**: Scrapes Amazon product pages for titles, prices, ratings, and review counts
- **Review Collection**: Uses SerpAPI's Google search engine to gather customer review snippets
- **PostgreSQL Integration**: Stores all data in a structured PostgreSQL database with SSL support
- **ASIN Parsing**: Automatically extracts Amazon Standard Identification Numbers from product URLs
- **Incremental Processing**: Smart ETL tracking to avoid reprocessing already-scraped products
- **Category Management**: Organize products by custom categories (e.g., "Search", "Experience")
- **CSV Export**: Download filtered review datasets by category or ASIN
- **Real-Time Monitoring**: Live status updates and progress tracking during ETL runs
- **Error Handling**: Robust error handling for API failures and parsing issues

## Architecture
```
┌─────────────────┐
│   Web Browser   │
│  (index.html)   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Flask API     │
│   (api.py)      │
└────────┬────────┘
         │
         ├──→ ETL Pipeline (etl_runner.py)
         │         │
         │         └──→ SerpAPI (Reviews)
         │
         ▼
┌─────────────────┐
│ PostgreSQL DB   │
│  amazon_links   │
│  amazon_reviews │
└─────────────────┘
```

## Prerequisites

- Python 3.8+
- PostgreSQL database (Aiven Cloud or self-hosted)
- SerpAPI account (free tier available at [serpapi.com](https://serpapi.com/))
- Modern web browser (Chrome, Firefox, Safari, Edge)

## Installation

1. **Clone the repository**:
```bash
git clone <repository-url>
cd amazon-reviews-etl
```

2. **Install dependencies**:
```bash
pip install -r requirements.txt
```

3. **Set up environment variables**:

Create a `.env` file in the project root:
```env
# PostgreSQL Configuration
PG_HOST=your_postgres_host
PG_PORT=5432
PG_DATABASE=defaultdb
PG_USER=your_username
PG_PASSWORD=your_password
PG_SSLMODE=require
PG_SSLROOTCERT=ca.pem

# SerpAPI Configuration
SERPAPI_KEY=your_serpapi_key
```

4. **Obtain SSL Certificate** (if using Aiven Cloud or SSL-enabled PostgreSQL):
   - Download the CA certificate from your database provider
   - Save it as `ca.pem` in the project root

## Database Setup

The database tables are created automatically when you first run the application. The schema includes:

### Tables

**amazon_links**
```sql
CREATE TABLE amazon_links (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE,
    asin TEXT,
    product_type TEXT,
    product_name TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

**amazon_reviews**
```sql
CREATE TABLE amazon_reviews (
    id SERIAL PRIMARY KEY,
    asin TEXT,
    product_url TEXT,
    product_name TEXT,
    review_title TEXT,
    review_text TEXT,
    rating TEXT,
    reviewer_name TEXT,
    review_date TEXT,
    verified BOOLEAN DEFAULT FALSE,
    inserted_at TIMESTAMP DEFAULT NOW()
);
```

**etl_log**
```sql
CREATE TABLE etl_log (
    asin TEXT PRIMARY KEY,
    last_extracted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Usage

### Starting the Application

**Windows:**
```bash
run_app.bat
```

**Manual Start:**
```bash
# Terminal 1: Start Flask API
python api.py

# Terminal 2: Open the dashboard
# Open index.html in your browser
```

The Flask API will run on `http://localhost:5000` and the dashboard will automatically open in your default browser.

### Dashboard Features

#### 1. Dashboard Overview
- Real-time statistics:
  - Total product links added
  - Processed products (ASINs)
  - Pending links awaiting extraction
  - Total reviews collected
  - Products with no reviews found
- Live progress bar during ETL processing
- Status messages with color-coded alerts
- Persistent Database Connection: Maintains a single PostgreSQL connection across all API routes and ETL tasks for improved performance.

#### 2. Add Amazon URLs
- Paste Amazon product URLs (one per line)
- Select a product category from dropdown
- Automatically extracts ASINs and validates URLs
- Supports bulk URL addition

#### 3. Process Reviews
- **Process New Products Only**: Skips already-processed ASINs (incremental mode)
- **Reprocess All Products**: Re-scrapes all products including previously processed ones
- Background processing with live status updates
- Automatic rate limiting (2-second delay between requests)

#### 4. Export Reviews
- Filter by product category or specific ASIN
- Download data as CSV with timestamped filename
- All reviews or filtered subsets
- Includes product metadata and review details

## API Endpoints

### Health Check
```http
GET /api/health
```
Returns API status and timestamp.

### Add Links
```http
POST /api/links
Content-Type: application/json

{
  "urls": ["https://www.amazon.com/dp/B08N5WRWNW", "..."],
  "product_type": "Search"
}
```

### Start ETL Processing
```http
POST /api/process
Content-Type: application/json

{
  "skip_existing": true
}
```

### Get Processing Status
```http
GET /api/status
```
Returns current processing status and database statistics.

### Export Reviews
```http
GET /api/export?product_type=Search&asin=B08N5WRWNW
```
Downloads CSV file with filtered reviews.

### Get Reviews (JSON)
```http
GET /api/reviews?product_type=Search&limit=100
```
Returns reviews as JSON for preview/API consumption.

### Get Categories
```http
GET /api/categories
```
Returns list of all product categories in the database.

## Project Structure
```
.
├── api.py               # Flask REST API server
├── index.html           # Web dashboard (frontend)
├── db_manager.py        # PostgreSQL database operations
├── etl_runner.py        # ETL orchestration logic
├── serpapi_client.py    # SerpAPI integration & HTML scraping
├── main.py              # Command-line ETL entry point
├── paapi_client.py      # Amazon PA-API client (optional)
├── requirements.txt     # Python dependencies
├── run_app.bat          # Windows launcher script
├── .env                 # Environment variables (not in repo)
├── ca.pem               # SSL certificate (not in repo)
└── README.md            # This file
```

## Module Descriptions

### `api.py`
Flask REST API server providing:
- CORS-enabled endpoints for frontend communication
- Background thread processing for ETL jobs
- CSV export functionality
- Real-time status monitoring
- Health check endpoint

### `index.html`
Web-based dashboard featuring:
- Modern gradient design with responsive layout
- Real-time statistics display
- Interactive forms for URL management
- Progress tracking with visual indicators
- Auto-refresh status updates every 3 seconds

### `db_manager.py`
PostgreSQL database operations:
- Connection management with SSL support
- Table creation and schema management
- Link and review insertion with deduplication
- ETL tracking for incremental processing
- Statistics aggregation for dashboard
- CSV export generation

### `etl_runner.py`
Core ETL orchestration:
- Processes all links from database
- Fetches metadata and reviews for each ASIN
- Implements incremental extraction logic
- Inserts only new reviews (avoids duplicates)
- Provides detailed logging and error tracking
- Returns summary statistics

### `serpapi_client.py`
Data extraction layer with two main functions:
- **`get_product_metadata()`**: Scrapes Amazon HTML for product details using BeautifulSoup
- **`get_reviews()`**: Uses SerpAPI Google search to find review snippets
- Implements realistic browser headers to avoid blocking
- Handles rate limiting and error scenarios

### `main.py`
Command-line entry point for running ETL pipeline without the web interface.

### `paapi_client.py`
Amazon Product Advertising API integration (optional):
- AWS Signature Version 4 authentication
- Fetches official product data (title, price, review stats)
- Alternative to web scraping for users with API access

## API Rate Limits

- **SerpAPI Free Tier**: 100 searches/month
- **HTML Scraping**: No official limit, but use responsibly with delays (implemented: 2s between requests)
- **Flask API**: No built-in rate limiting (runs locally)

## Data Flow

1. User adds Amazon URLs via dashboard → Stored in `amazon_links` table
2. User clicks "Process Reviews" → ETL pipeline starts in background thread
3. For each URL:
   - Extract ASIN from URL
   - Check `etl_log` table for last extraction date
   - Scrape product metadata from Amazon HTML
   - Fetch reviews via SerpAPI Google search
   - Filter new reviews (if incremental mode)
   - Insert reviews into `amazon_reviews` table
   - Update `etl_log` with extraction timestamp
4. User exports data → Generate CSV from `amazon_reviews` with optional filters

## Error Handling

The pipeline includes comprehensive error handling for:
- Invalid URLs or missing ASINs
- Network failures and timeouts
- API errors (SerpAPI rate limits, authentication failures)
- Database connection issues
- HTML parsing failures (Amazon layout changes)
- Concurrent processing conflicts

All errors are logged to console with descriptive messages and don't crash the pipeline.

## Incremental Processing

The system skips already-processed URLs by checking the amazon_reviews table for existing reviews.
When “Reprocess All” is selected, the system clears all reviews and reprocesses every link.

## CSV Export Format

Exported CSV includes the following columns:
- `asin`: Amazon product identifier
- `product_url`: Original Amazon product URL
- `product_name`: Full product title as scraped from Amazon
- `review_title`: Title of the customer review
- `review_text`: Full text content of the customer review
- `rating`: Numerical star rating (e.g., 1–5)
- `reviewer_name`: Name of the reviewer (if available)
- `review_date`: Date when the review was posted
- `verified`: Indicates whether the purchase was verified (True/False)
- `inserted_at`: Timestamp when the review was added to the database

## Troubleshooting

### Connection Errors
- Verify PostgreSQL credentials in `.env`
- Check SSL certificate path (should be `ca.pem` in project root)
- Ensure database is accessible from your network
- Test connection using `psql` or database client

### SerpAPI Errors
- Verify API key is active at [serpapi.com](https://serpapi.com/)
- Check remaining quota on SerpAPI dashboard
- Review error messages in Flask console output
- Ensure you're using `engine: google` (free tier)

### Missing Data
- Amazon HTML structure changes frequently; selectors may need updates in `serpapi_client.py`
- Some products may not have all fields (price, reviews)
- Try different products or inspect HTML manually
- Check Flask logs for parsing errors

### CORS Issues
- Ensure Flask API is running on `http://localhost:5000`
- Check browser console for CORS errors
- Verify `CORS(app)` is enabled in `api.py`

### Dashboard Not Updating
- Check if Flask API is running (`http://localhost:5000/api/health`)
- Open browser developer tools (F12) and check Console/Network tabs
- Verify `API_URL` constant in `index.html` matches your Flask server

## Performance Considerations

- **Processing Time**: ~5-10 seconds per product (includes scraping + API calls + delays)
- **Memory Usage**: Minimal (< 100MB for typical workloads)
- **Database**: PostgreSQL handles millions of reviews efficiently with proper indexing
- **Concurrency**: Single-threaded ETL to respect rate limits; API supports concurrent dashboard users

## Future Enhancements

Potential upgrades (dependencies already included):

- **Sentiment Analysis**: Use `vaderSentiment` and `textblob` for review sentiment scoring
- **Scheduling**: Implement automated daily/weekly scraping with `loguru`
- **Connection Pooling**: Currently uses one persistent connection; future versions will use psycopg_pool for concurrent scalability.
- **Data Validation**: Add schema validation for incoming data
- **Advanced Filtering**: Multi-dimensional filtering in dashboard
- **Visualization**: Add charts for rating distribution, sentiment trends
- **User Authentication**: Add login system for multi-user deployments
- **Notification System**: Email/webhook alerts when processing completes

## Legal & Ethical Considerations

**Important Notes**:

1. **Amazon Terms of Service**: Web scraping may violate Amazon's ToS. Use official APIs when possible.
2. **Rate Limiting**: The system implements 2-second delays between requests to avoid overloading servers.
3. **Personal Use**: This tool is for educational/research purposes only.
4. **Robots.txt**: Respect Amazon's robots.txt directives.
5. **Data Privacy**: Handle user reviews responsibly and comply with data protection regulations (GDPR, CCPA).
6. **Fair Use**: Do not use scraped data for commercial purposes without proper authorization.
7. **SerpAPI Terms**: Comply with SerpAPI's usage policies and rate limits.

## Security Considerations

- **Never commit `.env` file**: Contains sensitive credentials
- **SSL/TLS**: Always use SSL for PostgreSQL connections (especially cloud databases)
- **API Keys**: Rotate keys periodically
- **CORS**: Currently allows all origins; restrict in production
- **Input Validation**: URLs are validated before processing
- **SQL Injection**: Uses parameterized queries (safe from SQL injection)

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes with tests
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Submit a pull request

## License

This project was created as part of the MSBA program at William & Mary for educational purposes only.

**Academic Use Only** - This software is provided for educational and research purposes. 
Commercial use or redistribution without permission is not permitted.

© 2025 Suyog Mainali, William & Mary MSBA Program 

## Support

For issues or questions:
- Open an issue on GitHub
- Check existing issues for solutions
- Review Amazon and SerpAPI documentation
- Contact: mainalisuyog0@gmail.com

## Acknowledgments

- [SerpAPI](https://serpapi.com/) for search API access
- [Flask](https://flask.palletsprojects.com/) for the web framework
- [psycopg](https://www.psycopg.org/) for PostgreSQL connectivity
- [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/) for HTML parsing
- [Aiven](https://aiven.io/) for managed PostgreSQL hosting
- William & Mary MSBA Program for project guidance

---

**Version**: 1.0.0  
**Last Updated**: October 2025  
**Python**: 3.8+  
**Status**: Active Development