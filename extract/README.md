# Extract Service

This service contains scrapers for data collection from various international statistical sources. All scrapers are fully implemented and operational.

## Structure

Each scraper is organized in its own subdirectory with:
- `scraper.py` - Main scraper implementation
- `requirements.txt` - Python dependencies
- `Dockerfile` - Container configuration
- `__init__.py` - Package initialization

## Scrapers

### World Bank CPI (`wb/`)

Fetches Consumer Price Index (CPI) data from the World Bank REST API (`FP.CPI.TOTL` indicator).

- **API**: `https://api.worldbank.org/v2/country/{code}/indicator/FP.CPI.TOTL`
- **Format**: JSON
- **Libraries**: `requests`

### OECD Wages (`oecd/`)

Fetches average annual wage data from the OECD SDMX-JSON API (`AV_AN_WAGE` dataset, USD).

- **API**: `https://stats.oecd.org/SDMX-JSON/data/AV_AN_WAGE/...`
- **Format**: SDMX-JSON
- **Libraries**: `requests`

### ILOSTAT Wages (`ilostat/`)

Fetches average hourly earnings from the ILOSTAT SDMX API (`DF_EAR_4HRL_SEX_CUR_NB` dataflow). Used as a backup source for countries not covered by OECD.

- **API**: `https://sdmx.ilo.org/rest/data/ILO,DF_EAR_4HRL_SEX_CUR_NB,1.0/all`
- **Format**: SDMX-JSON
- **Libraries**: `requests`

### FAOSTAT Food CPI (`faostat/`)

Fetches Food Consumer Price Index data from the FAOSTAT bulk download (item code `23013`, base year 2015 = 100).

- **Source**: `https://bulks-faostat.fao.org/production/ConsumerPriceIndices_E_All_Data_(Normalized).zip`
- **Format**: ZIP archive containing CSV
- **Libraries**: `requests`, `pandas`

### BIS Housing Prices (`bis/`)

Fetches residential property price per square meter data from BIS bulk downloads (CSV/XLSX/ZIP).

- **Source**: `https://www.bis.org/statistics/pp_selected_csv.zip` (+ fallback URLs)
- **Format**: ZIP/XLSX/CSV
- **Libraries**: `requests`, `pandas`, `openpyxl`

## Libraries

| Library | Purpose | Used by |
|---------|---------|---------|
| `requests` | HTTP requests to APIs and bulk download endpoints | all scrapers |
| `pandas` | Parsing CSV/Excel data, DataFrame operations | BIS, FAOSTAT |
| `openpyxl` | Reading Excel (.xlsx) files | BIS |
| `json` (stdlib) | Parsing JSON / SDMX-JSON API responses | OECD, ILOSTAT, World Bank |
| `zipfile` (stdlib) | Extracting CSV/XLSX from ZIP archives | BIS, FAOSTAT |
| `io` (stdlib) | In-memory byte/string streams for file parsing | BIS, FAOSTAT |
