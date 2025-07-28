# Stock Recommendations Scraper

A Python web scraping service that fetches broker recommendations from MoneyControl. Built because manually checking stock recommendations across different brokers is time-consuming.

## Quick Start

### Local Development
```bash
poetry install
poetry run python app.py
```
The server will be available at http://localhost:5000

### Docker
```bash
docker build -t broker-recommendations .
# For development (uses default API key)
docker run -p 5000:5000 broker-recommendations

# For production (set your own API key)
docker run -p 5000:5000 -e API_KEY=your-secure-api-key broker-recommendations
```

## API Usage

All endpoints except `/health` require an API key in the `X-API-Key` header.

### Get Recommendations
```bash
curl -H "X-API-Key: dev-api-key-123" http://localhost:5000/recommendations
```
Returns the latest broker recommendations scraped from MoneyControl.

**What you get:**
- Real broker names (Motilal Oswal, HDFC Securities, etc.)
- Stock recommendations (BUY/SELL/HOLD)
- Target prices when available
- Current market prices when available

### Health Check
```bash
curl http://localhost:5000/health
```
Simple health check - no authentication needed.

### Analytics
```bash
curl -H "X-API-Key: dev-api-key-123" http://localhost:5000/stats
curl -H "X-API-Key: dev-api-key-123" http://localhost:5000/top-companies  
```

## Configuration

Set these environment variables if needed:
- `PORT`: Server port (default: 5000)
- `CHROME_BIN`: Chrome binary path (for Docker)
- `CHROMEDRIVER_PATH`: ChromeDriver path (for Docker)

## How It Works

The scraper targets MoneyControl's stock ideas page and extracts recommendations using:
- **HTTP requests** with browser headers to avoid bot detection
- **BeautifulSoup** for HTML parsing and data extraction
- **Selenium** as a fallback (currently disabled due to anti-bot measures)
- **Flask** for the REST API

Scraping MoneyControl is tricky because they have bot protection, so the crawler uses multiple strategies to get data.