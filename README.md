# Amazon Reviews ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline for collecting Amazon product metadata and customer reviews. This pipeline combines web scraping, SerpAPI integration, and PostgreSQL storage to build a comprehensive reviews database.

## Features

- **Product Metadata Extraction**: Scrapes Amazon product pages for titles, prices, ratings, and review counts
- **Review Collection**: Uses SerpAPI's Google search engine to gather customer review snippets
- **PostgreSQL Integration**: Stores all data in a structured PostgreSQL database
- **ASIN Parsing**: Automatically extracts Amazon Standard Identification Numbers from product URLs
- **Error Handling**: Robust error handling for API failures and parsing issues
- **Optional PA-API Support**: Includes Amazon Product Advertising API client for official data access

## Architecture

```
┌─────────────┐
│  amazon_    │
│   links     │  ← Source URLs
│  (table)    │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│ ETL Pipeline│
│             │
│  • Extract  │──→ SerpAPI (Reviews)
│  • Parse    │──→ HTML Scraping (Metadata)
│  • Load     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  amazon_    │
│  reviews    │  ← Stored Data
│  (table)    │
└─────────────┘
```

## Prerequisites

- Python 3.8+
- PostgreSQL database
- SerpAPI account (free tier available)
- (Optional) Amazon Product Advertising API credentials

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd amazon-reviews-etl
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:

Create a `.env` file in the project root:

```env
# PostgreSQL Configuration
PG_HOST=your_postgres_host
PG_PORT=5432
PG_DATABASE=your_database_name
PG_USER=your_username
PG_PASSWORD=your_password
PG_SSLMODE=require
PG_SSLROOTCERT=/path/to/cert.pem

# SerpAPI Configuration
SERPAPI_KEY=your_serpapi_key

# Optional: Amazon Product Advertising API
PAAPI_ACCESS_KEY=your_access_key
PAAPI_SECRET_KEY=your_secret_key
PAAPI_ASSOC_TAG=your_associate_tag
```

## Database Setup

Create the required PostgreSQL tables:

```sql
-- Source URLs table
CREATE TABLE amazon_links (
    id SERIAL PRIMARY KEY,
    url TEXT NOT NULL,
    product_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reviews table
CREATE TABLE amazon_reviews (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(20) NOT NULL,
    product_url TEXT,
    title TEXT,
    price VARCHAR(50),
    avg_star_rating DECIMAL(2,1),
    total_reviews INTEGER,
    review_title TEXT,
    review_text TEXT,
    rating INTEGER,
    review_date VARCHAR(50),
    verified BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better query performance
CREATE INDEX idx_asin ON amazon_reviews(asin);
CREATE INDEX idx_rating ON amazon_reviews(rating);
CREATE INDEX idx_created_at ON amazon_reviews(created_at);
```

## Usage

### Basic Usage

Run the ETL pipeline:

```bash
python main.py
```

The pipeline will:
1. Fetch up to 50 URLs from the `amazon_links` table
2. Extract ASIN from each URL
3. Scrape product metadata (title, price, rating, review count)
4. Fetch customer reviews via SerpAPI
5. Store all data in the `amazon_reviews` table

### Customizing Batch Size

Modify the `limit` parameter in `etl_runner.py`:

```python
links = self.db.fetch_links(limit=100)  # Process 100 URLs
```

## Project Structure

```
.
├── main.py              # Entry point
├── etl_runner.py        # ETL orchestration logic
├── db_manager.py        # PostgreSQL database operations
├── serpapi_client.py    # SerpAPI integration & HTML scraping
├── paapi_client.py      # Amazon PA-API client (optional)
├── requirements.txt     # Python dependencies
├── .env                 # Environment variables (not in repo)
└── README.md           # This file
```

## Module Descriptions

### `main.py`
Simple entry point that initializes and runs the ETL pipeline.

### `etl_runner.py`
Core ETL logic including:
- ASIN extraction from URLs
- Data cleaning and normalization
- Orchestration of data fetching and storage

### `db_manager.py`
PostgreSQL database operations:
- Connection management with SSL support
- Fetching source URLs
- Inserting review records
- Auto-commit mode for reliability

### `serpapi_client.py`
Two main functions:
- **`get_product_metadata()`**: Scrapes Amazon HTML for product details
- **`get_reviews()`**: Uses SerpAPI Google search to find review snippets

### `paapi_client.py`
Amazon Product Advertising API integration:
- AWS Signature Version 4 authentication
- Fetches official product data (title, price, review stats)
- Alternative to web scraping for users with API access

## API Rate Limits

- **SerpAPI Free Tier**: 100 searches/month
- **HTML Scraping**: No official limit, but use responsibly with delays
- **PA-API**: Varies by account type (typically 8,640 requests/day)

## Error Handling

The pipeline includes error handling for:
- Invalid URLs or missing ASINs
- Network failures
- API errors (SerpAPI, PA-API)
- Database connection issues
- HTML parsing failures

Errors are logged to console with descriptive messages.

## Future Enhancements

Potential upgrades included in dependencies:

- **Sentiment Analysis**: Uses `vaderSentiment` and `textblob` for review sentiment scoring
- **Scheduling**: Implement with `loguru` for scheduled data collection
- **Connection Pooling**: Scale with `psycopg` pool support
- **Data Validation**: Add schema validation for incoming data
- **Duplicate Detection**: Check for existing ASINs before inserting

## Legal & Ethical Considerations

**Important Notes**:

1. **Amazon Terms of Service**: Web scraping may violate Amazon's ToS. Use official APIs when possible.
2. **Rate Limiting**: Implement delays between requests to avoid overloading servers.
3. **Personal Use**: This tool is for educational/research purposes.
4. **Robots.txt**: Respect Amazon's robots.txt directives.
5. **Data Privacy**: Handle user reviews responsibly and comply with data protection regulations.

## Troubleshooting

### Connection Errors
- Verify PostgreSQL credentials in `.env`
- Check SSL certificate path
- Ensure database is accessible from your network

### SerpAPI Errors
- Verify API key is active
- Check remaining quota at serpapi.com
- Review error messages in console output

### Missing Data
- Amazon HTML structure changes frequently; selectors may need updates
- Some products may not have all fields (price, reviews)
- Try different products or check HTML manually

## Contributing

Contributions are welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

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

## Acknowledgments

- [SerpAPI](https://serpapi.com/) for search API access
- [psycopg](https://www.psycopg.org/) for PostgreSQL connectivity
- [Beautiful Soup](https://www.crummy.com/software/BeautifulSoup/) for HTML parsing