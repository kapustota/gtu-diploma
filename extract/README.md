# Extract Service

This service contains scrapers for data collection from various sources.

## Structure

Each scraper is organized in its own subdirectory with:
- `scraper.py` - Main scraper implementation
- `requirements.txt` - Python dependencies
- `Dockerfile` - Container configuration
- `__init__.py` - Package initialization

## Scrapers

### WorldBank CPI (`wb/`)

Fetches Consumer Price Index (CPI) data from WorldBank API.

**Building:**
```bash
docker build -t scraper-wb:latest -f extract/wb/Dockerfile extract/wb/
```

**Running:**
```bash
docker run --rm scraper-wb:latest
```

Or using docker-compose:
```bash
docker-compose up scraper-wb
```

**Testing:**
```bash
python test_scraper_wb.py
```

### Future Scrapers

- `oecd/` - OECD wages scraper
- `ilostat/` - ILOSTAT wages scraper
- `faostat/` - FAOSTAT food CPI scraper
- `bis/` - BIS housing price scraper
- `wb/` - World Bank inflation scraper

