"""
BIS Housing Price Scraper
Fetches residential property price per square meter data from BIS
BIS provides data in CSV format via bulk download
"""

import requests
import pandas as pd
import io
import zipfile
import time
from typing import List, Dict, Optional
from datetime import datetime
import openpyxl  # For Excel support


class BISHousingPriceScraper:
    """
    Scraper for BIS residential property prices (price per square meter)
    BIS provides data via bulk download in CSV format
    """
    
    # BIS bulk download URLs for residential property prices
    # BIS provides data in multiple formats - we'll try CSV first
    BULK_DOWNLOAD_URLS = [
        "https://www.bis.org/statistics/pp_selected_csv.zip",
        "https://data.bis.org/bulkdownload/pp_selected_csv.zip",
        "https://www.bis.org/statistics/pp_selected.xlsx",
        "https://data.bis.org/bulkdownload/pp_selected.xlsx"
    ]
    
    def __init__(self):
        """Initialize scraper"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0'
        })
        self._data_cache = None
    
    def _download_and_parse_data(self) -> pd.DataFrame:
        """
        Download and parse BIS residential property price data
        
        Returns:
            DataFrame with housing price data
        """
        if self._data_cache is not None:
            return self._data_cache
        
        max_retries = 3
        retry_delay = 2
        
        # Try multiple possible URLs
        urls_to_try = self.BULK_DOWNLOAD_URLS
        
        for url in urls_to_try:
            for attempt in range(max_retries):
                try:
                    print(f"Downloading BIS housing price data from {url}...")
                    response = self.session.get(url, timeout=300, stream=True)
                    
                    if response.status_code == 429:
                        if attempt < max_retries - 1:
                            wait_time = retry_delay * (attempt + 1)
                            print(f"Rate limited, waiting {wait_time}s before retry...")
                            time.sleep(wait_time)
                            continue
                        else:
                            print("Rate limited after multiple attempts")
                            continue
                    
                    if response.status_code == 404:
                        print(f"URL not found: {url}, trying next URL...")
                        break
                    
                    response.raise_for_status()
                    
                    # Check if response is ZIP, XLSX, or CSV
                    content_type = response.headers.get('Content-Type', '').lower()
                    
                    if 'zip' in content_type or url.endswith('.zip'):
                        # Extract CSV/XLSX from ZIP
                        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                            # Find CSV or XLSX file in ZIP
                            csv_files = [f for f in zip_file.namelist() if f.endswith(('.csv', '.xlsx'))]
                            if not csv_files:
                                print(f"No CSV/XLSX file found in ZIP from {url}")
                                break
                            
                            data_file = csv_files[0]
                            with zip_file.open(data_file) as f:
                                if data_file.endswith('.csv'):
                                    df = pd.read_csv(f, encoding='utf-8')
                                else:
                                    df = pd.read_excel(f)
                    elif 'excel' in content_type or url.endswith('.xlsx'):
                        # Direct XLSX
                        df = pd.read_excel(io.BytesIO(response.content))
                    else:
                        # Direct CSV
                        df = pd.read_csv(io.StringIO(response.text), encoding='utf-8')
                    
                    # Cache the data
                    self._data_cache = df
                    print(f"Downloaded and parsed {len(df)} records from {url}")
                    return df
                    
                except requests.exceptions.RequestException as e:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    print(f"Error downloading from {url}: {e}")
                    break
                except (zipfile.BadZipFile, pd.errors.EmptyDataError, Exception) as e:
                    print(f"Error processing data from {url}: {e}")
                    break
        
        # If all URLs failed, try to parse from the PDF endpoint mentioned in requirements
        # This is a fallback - we'll try to extract data from HTML if available
        print("Trying alternative method: checking for HTML/table data...")
        try:
            # Try to get data from BIS statistics page
            html_url = "https://www.bis.org/statistics/tables_h.htm"
            response = self.session.get(html_url, timeout=30)
            if response.status_code == 200:
                # Try to parse HTML tables
                dfs = pd.read_html(response.text)
                if dfs:
                    df = dfs[0]
                    self._data_cache = df
                    print(f"Parsed {len(df)} records from HTML table")
                    return df
        except Exception as e:
            print(f"Alternative method also failed: {e}")
        
        raise Exception("Failed to download BIS data from all available sources")
    
    def _normalize_country_code(self, country_name: str, country_code: Optional[str] = None) -> tuple:
        """
        Normalize country code to ISO 3-letter format
        
        Args:
            country_name: Country name from BIS data
            country_code: Optional country code from BIS data
        
        Returns:
            Tuple of (normalized_country_code, country_name)
        """
        # BIS may use different country codes, we need to map them
        # Common mappings
        country_mapping = {
            'United States': ('USA', 'United States'),
            'United Kingdom': ('GBR', 'United Kingdom'),
            'Germany': ('DEU', 'Germany'),
            'France': ('FRA', 'France'),
            'Japan': ('JPN', 'Japan'),
            'China': ('CHN', 'China'),
            'Russia': ('RUS', 'Russia'),
            'Canada': ('CAN', 'Canada'),
            'Australia': ('AUS', 'Australia'),
            'Italy': ('ITA', 'Italy'),
            'Spain': ('ESP', 'Spain'),
            'Netherlands': ('NLD', 'Netherlands'),
            'Switzerland': ('CHE', 'Switzerland'),
            'Sweden': ('SWE', 'Sweden'),
            'Norway': ('NOR', 'Norway'),
            'Denmark': ('DNK', 'Denmark'),
            'Belgium': ('BEL', 'Belgium'),
            'Austria': ('AUT', 'Austria'),
            'Poland': ('POL', 'Poland'),
            'South Korea': ('KOR', 'South Korea'),
            'India': ('IND', 'India'),
            'Brazil': ('BRA', 'Brazil'),
            'Mexico': ('MEX', 'Mexico'),
            'South Africa': ('ZAF', 'South Africa'),
            'Turkey': ('TUR', 'Turkey'),
            'Argentina': ('ARG', 'Argentina'),
            'Chile': ('CHL', 'Chile'),
            'Colombia': ('COL', 'Colombia'),
            'Peru': ('PER', 'Peru'),
            'Indonesia': ('IDN', 'Indonesia'),
            'Malaysia': ('MYS', 'Malaysia'),
            'Thailand': ('THA', 'Thailand'),
            'Philippines': ('PHL', 'Philippines'),
            'Singapore': ('SGP', 'Singapore'),
            'Hong Kong': ('HKG', 'Hong Kong'),
            'New Zealand': ('NZL', 'New Zealand'),
            'Ireland': ('IRL', 'Ireland'),
            'Portugal': ('PRT', 'Portugal'),
            'Greece': ('GRC', 'Greece'),
            'Finland': ('FIN', 'Finland'),
            'Czech Republic': ('CZE', 'Czech Republic'),
            'Hungary': ('HUN', 'Hungary'),
            'Romania': ('ROU', 'Romania'),
            'Bulgaria': ('BGR', 'Bulgaria'),
            'Croatia': ('HRV', 'Croatia'),
            'Slovakia': ('SVK', 'Slovakia'),
            'Slovenia': ('SVN', 'Slovenia'),
            'Estonia': ('EST', 'Estonia'),
            'Latvia': ('LVA', 'Latvia'),
            'Lithuania': ('LTU', 'Lithuania'),
        }
        
        # Try to find match
        country_name_clean = str(country_name).strip()
        for key, (code, name) in country_mapping.items():
            if key.lower() in country_name_clean.lower() or country_name_clean.lower() in key.lower():
                return (code, name)
        
        # If country_code is provided and looks like ISO code, use it
        if country_code and len(str(country_code).strip()) == 3:
            return (str(country_code).strip().upper(), country_name_clean)
        
        # Fallback: use first 3 letters of country name (not ideal but works)
        code = country_name_clean[:3].upper() if len(country_name_clean) >= 3 else country_name_clean.upper()
        return (code, country_name_clean)
    
    def get_price_data(self, country_code: Optional[str] = None) -> List[Dict]:
        """
        Fetch housing price per square meter data
        
        Args:
            country_code: ISO 3-letter country code (optional, None = all countries)
        
        Returns:
            List of dictionaries with price data
        """
        try:
            df = self._download_and_parse_data()
            
            # BIS CSV/XLSX structure may vary, we need to identify columns
            # Common column names in BIS data:
            # - Country/Country name/REF_AREA/Jurisdiction
            # - Time/Period/Year/Date
            # - Value/Price/Price per m2/Index/Level
            
            # First, check if data is in wide format (countries as rows, years as columns)
            # or long format (country, year, value)
            
            # Find relevant columns
            country_col = None
            time_col = None
            value_col = None
            
            # Try to find country column
            for col in df.columns:
                col_lower = str(col).lower().strip()
                if any(x in col_lower for x in ['country', 'area', 'ref_area', 'jurisdiction', 'location']):
                    country_col = col
                    break
            
            # Try to find time column
            for col in df.columns:
                col_lower = str(col).lower().strip()
                if any(x in col_lower for x in ['time', 'period', 'year', 'date']):
                    time_col = col
                    break
            
            # Try to find value column
            for col in df.columns:
                col_lower = str(col).lower().strip()
                if any(x in col_lower for x in ['value', 'price', 'index', 'level', 'pp_selected']):
                    # Skip if it's clearly not a value column
                    if any(x in col_lower for x in ['code', 'id', 'name', 'description']):
                        continue
                    value_col = col
                    break
            
            # If we have country and time but no value, might be wide format
            # Check if columns after country/time are numeric (years)
            if country_col and time_col and not value_col:
                numeric_cols = []
                for col in df.columns:
                    if col in [country_col, time_col]:
                        continue
                    # Check if column name is a year or if values are numeric
                    try:
                        int(str(col).strip())
                        numeric_cols.append(col)
                    except:
                        if df[col].dtype in ['float64', 'int64']:
                            numeric_cols.append(col)
                
                if numeric_cols:
                    # Wide format - need to melt
                    print("Detected wide format data, converting to long format...")
                    id_vars = [country_col]
                    if time_col in df.columns:
                        id_vars.append(time_col)
                    
                    df = pd.melt(df, id_vars=id_vars, value_vars=numeric_cols, 
                                var_name='year', value_name='price_per_m2')
                    time_col = 'year'
                    value_col = 'price_per_m2'
            
            if not all([country_col, time_col, value_col]):
                # Try to infer from first few rows
                print("Could not auto-detect columns, trying to infer from data structure...")
                print(f"Available columns: {list(df.columns)}")
                print(f"First few rows:\n{df.head()}")
                # Use first few columns as fallback
                if len(df.columns) >= 3:
                    country_col = df.columns[0]
                    time_col = df.columns[1]
                    value_col = df.columns[2]
                else:
                    raise ValueError(f"Cannot identify required columns in BIS data. Columns: {list(df.columns)}")
            
            parsed = []
            for _, row in df.iterrows():
                try:
                    country_name_raw = str(row[country_col]).strip()
                    time_value = str(row[time_col]).strip()
                    price_value = row[value_col]
                    
                    # Skip if price is missing
                    if pd.isna(price_value):
                        continue
                    
                    # Parse year
                    year = None
                    if time_value.isdigit():
                        year = int(time_value)
                    elif '-' in time_value:
                        # Try to extract year from date string
                        parts = time_value.split('-')
                        for part in parts:
                            if part.isdigit() and len(part) == 4:
                                year = int(part)
                                break
                    
                    if not year or year < 1980 or year > datetime.now().year + 1:
                        continue
                    
                    # Normalize country code
                    country_code_norm, country_name = self._normalize_country_code(country_name_raw)
                    
                    # Filter by country if specified
                    if country_code:
                        country_code_upper = str(country_code).strip().upper()
                        if country_code_norm != country_code_upper:
                            # Also try matching by name
                            if country_code_upper not in country_name_raw.upper():
                                continue
                    
                    parsed.append({
                        'country_code': country_code_norm,
                        'country_name': country_name,
                        'year': year,
                        'price_per_m2': float(price_value),
                        'indicator': 'Residential property price per square meter',
                        'source': 'BIS'
                    })
                except (ValueError, TypeError, KeyError) as e:
                    continue
            
            return sorted(parsed, key=lambda x: (x['country_code'], x['year']))
            
        except Exception as e:
            print(f"Error fetching BIS housing price data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_price_data_multiple_countries(self, country_codes: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch price data for multiple countries
        
        Args:
            country_codes: List of ISO 3-letter country codes
        
        Returns:
            Dictionary mapping country codes to their price data
        """
        # Download data once for all countries
        df = self._download_and_parse_data()
        
        results = {}
        for country_code in country_codes:
            print(f"Filtering price data for {country_code}...")
            results[country_code] = self.get_price_data(country_code)
        
        return results
    
    def get_all_countries_prices(self) -> Dict[str, List[Dict]]:
        """
        Fetch price data for all available countries
        Gets all available years from the earliest available for each country
        
        Returns:
            Dictionary mapping country codes to their price data
        """
        print("Fetching housing price data for all countries...")
        all_data = self.get_price_data()
        
        # Group by country code
        grouped = {}
        for record in all_data:
            country_code = record['country_code']
            if country_code not in grouped:
                grouped[country_code] = []
            grouped[country_code].append(record)
        
        # Sort each country's records by year
        for country_code in grouped:
            grouped[country_code] = sorted(grouped[country_code], key=lambda x: x['year'])
        
        print(f"Found data for {len(grouped)} countries")
        return grouped


if __name__ == "__main__":
    # Test the scraper
    scraper = BISHousingPriceScraper()
    
    # Test with a few countries
    test_countries = ['USA', 'GBR', 'DEU', 'JPN', 'FRA']
    print("Testing BIS Housing Price Scraper")
    print("=" * 50)
    
    for country in test_countries:
        print(f"\nFetching data for {country}...")
        data = scraper.get_price_data(country)
        
        if data:
            print(f"  Found {len(data)} records")
            print(f"  Years range: {data[0]['year']} - {data[-1]['year']}")
            print(f"  Sample record (first): {data[0]}")
            if len(data) > 1:
                print(f"  Sample record (last): {data[-1]}")
        else:
            print(f"  No data found for {country}")
