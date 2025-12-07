"""
FAOSTAT Food CPI Scraper
Fetches Food Consumer Price Index (CPI) data from FAOSTAT bulk download
"""

import requests
import pandas as pd
import io
import zipfile
import time
from typing import List, Dict, Optional
from datetime import datetime


class FAOSTATFoodCPIScraper:
    """
    Scraper for FAOSTAT Food CPI data
    Uses bulk download CSV: ConsumerPriceIndices_E_All_Data_(Normalized).zip
    """
    
    BULK_DOWNLOAD_URL = "https://bulks-faostat.fao.org/production/ConsumerPriceIndices_E_All_Data_(Normalized).zip"
    
    def __init__(self):
        """Initialize scraper"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0'
        })
        self._data_cache = None
    
    def _download_and_parse_data(self) -> pd.DataFrame:
        """
        Download and parse FAOSTAT bulk data
        
        Returns:
            DataFrame with Food CPI data
        """
        if self._data_cache is not None:
            return self._data_cache
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                print("Downloading FAOSTAT Food CPI data...")
                response = self.session.get(self.BULK_DOWNLOAD_URL, timeout=300, stream=True)
                
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        print(f"Rate limited, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception("Rate limited after multiple attempts")
                
                response.raise_for_status()
                
                # Extract CSV from ZIP
                with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
                    csv_file = zip_file.namelist()[0]  # Get first CSV file
                    with zip_file.open(csv_file) as f:
                        df = pd.read_csv(f, encoding='utf-8')
                
                # Cache the data
                self._data_cache = df
                print(f"Downloaded and parsed {len(df)} records")
                return df
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                print(f"Error downloading data: {e}")
                raise
            except Exception as e:
                print(f"Error processing data: {e}")
                raise
        
        raise Exception("Failed to download data after multiple attempts")
    
    def get_food_cpi_data(self, country_code: Optional[str] = None) -> List[Dict]:
        """
        Fetch Food CPI data
        
        Args:
            country_code: ISO 3-letter country code (optional, None = all countries)
        
        Returns:
            List of dictionaries with Food CPI data
        """
        try:
            df = self._download_and_parse_data()
            
            # Filter for Food CPI indicator first
            # Item Code 23013 = "Consumer Prices, Food Indices (2015 = 100)"
            food_cpi_df = df[df['Item Code'] == 23013].copy()
            
            # Filter by country if specified
            if country_code:
                # Clean country code (remove quotes and apostrophes)
                country_code_clean = str(country_code).strip().strip("'\"")
                # Clean the M49 codes by removing quotes and apostrophes
                food_cpi_df['Area Code (M49) Clean'] = food_cpi_df['Area Code (M49)'].astype(str).str.strip().str.strip("'\"")
                food_cpi_df['Area Code Clean'] = food_cpi_df['Area Code'].astype(str).str.strip()
                
                # Filter strictly by Area Code (M49) first, then by Area Code, then by Area name
                mask = (
                    (food_cpi_df['Area Code (M49) Clean'] == country_code_clean) |
                    (food_cpi_df['Area Code Clean'] == country_code_clean)
                )
                
                # If no match by code, try by name
                if not mask.any():
                    mask = food_cpi_df['Area'].astype(str).str.contains(country_code_clean, case=False, na=False)
                
                filtered_df = food_cpi_df[mask].copy()
                
                # Verify all records are for the same country
                if len(filtered_df) > 0:
                    unique_countries = filtered_df['Area Code (M49) Clean'].unique()
                    if len(unique_countries) > 1:
                        # If multiple countries, use the most common one
                        most_common = filtered_df['Area Code (M49) Clean'].mode()[0]
                        filtered_df = filtered_df[filtered_df['Area Code (M49) Clean'] == most_common].copy()
            else:
                filtered_df = food_cpi_df.copy()
            
            parsed = []
            for _, row in filtered_df.iterrows():
                try:
                    year = int(row['Year'])
                    value = row['Value']
                    
                    if pd.notna(value):
                        # Clean country code - remove quotes and apostrophes
                        area_code_m49 = str(row.get('Area Code (M49)', '')).strip().strip("'\"")
                        
                        parsed.append({
                            'country_code': area_code_m49,
                            'country_name': str(row.get('Area', '')).strip(),
                            'year': year,
                            'cpi_food': float(value),
                            'indicator': str(row.get('Item', 'Food CPI')).strip(),
                            'source': 'FAOSTAT'
                        })
                except (ValueError, TypeError, KeyError):
                    continue
            
            return sorted(parsed, key=lambda x: (x['country_code'], x['year']))
            
        except Exception as e:
            print(f"Error fetching Food CPI data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_food_cpi_data_multiple_countries(self, country_codes: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch Food CPI data for multiple countries
        
        Args:
            country_codes: List of ISO 3-letter country codes or FAOSTAT area codes
        
        Returns:
            Dictionary mapping country codes to their Food CPI data
        """
        # Download data once for all countries
        df = self._download_and_parse_data()
        
        results = {}
        for country_code in country_codes:
            print(f"Filtering Food CPI data for {country_code}...")
            results[country_code] = self.get_food_cpi_data(country_code)
        
        return results
    
    def get_all_countries_food_cpi(self) -> Dict[str, List[Dict]]:
        """
        Fetch Food CPI data for all available countries
        Gets all available years from the earliest available for each country
        
        Returns:
            Dictionary mapping country codes to their Food CPI data
        """
        print("Fetching Food CPI data for all countries...")
        all_data = self.get_food_cpi_data()
        
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
    scraper = FAOSTATFoodCPIScraper()
    
    # Test with a few countries (using M49 ISO numeric codes)
    # 840 = USA, 643 = RUS, 276 = DEU, 826 = GBR, 392 = JPN
    test_countries = ['840', '643', '276', '826', '392']
    print("Testing FAOSTAT Food CPI Scraper")
    print("=" * 50)
    
    for country in test_countries:
        print(f"\nFetching data for {country}...")
        data = scraper.get_food_cpi_data(country)
        
        if data:
            print(f"  Found {len(data)} records")
            print(f"  Years range: {data[0]['year']} - {data[-1]['year']}")
            print(f"  Sample record (first): {data[0]}")
            if len(data) > 1:
                print(f"  Sample record (last): {data[-1]}")
        else:
            print(f"  No data found for {country}")

