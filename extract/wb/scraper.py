"""
WorldBank CPI Scraper
Fetches Consumer Price Index (CPI) data from WorldBank API
"""

import requests
import json
from typing import List, Dict, Optional
from datetime import datetime


class WorldBankCPIScraper:
    """
    Scraper for WorldBank CPI data
    API endpoint: https://api.worldbank.org/v2/country/{country}/indicator/{indicator}
    """
    
    BASE_URL = "https://api.worldbank.org/v2"
    CPI_INDICATOR = "FP.CPI.TOTL"  # Consumer Price Index (all items)
    
    def __init__(self, format: str = "json", per_page: int = 20000):
        """
        Initialize scraper
        
        Args:
            format: Response format (json, xml, csv)
            per_page: Number of records per page
        """
        self.format = format
        self.per_page = per_page
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0'
        })
    
    def get_cpi_data(self, country_code: str, start_year: Optional[int] = None, 
                     end_year: Optional[int] = None) -> List[Dict]:
        """
        Fetch CPI data for a specific country
        
        Args:
            country_code: ISO 3-letter country code (e.g., 'USA', 'RUS')
            start_year: Start year (optional)
            end_year: End year (optional)
        
        Returns:
            List of dictionaries with CPI data
        """
        url = f"{self.BASE_URL}/country/{country_code}/indicator/{self.CPI_INDICATOR}"
        params = {
            'format': self.format,
            'per_page': self.per_page
        }
        
        if start_year:
            params['date'] = f"{start_year}:{end_year or datetime.now().year}"
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # WorldBank API returns data in format: [metadata, [data_array]]
            if len(data) >= 2 and isinstance(data[1], list):
                return self._parse_cpi_records(data[1])
            else:
                return []
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {country_code}: {e}")
            return []
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"Error parsing response for {country_code}: {e}")
            return []
    
    def get_cpi_data_multiple_countries(self, country_codes: List[str], 
                                        start_year: Optional[int] = None,
                                        end_year: Optional[int] = None) -> Dict[str, List[Dict]]:
        """
        Fetch CPI data for multiple countries
        
        Args:
            country_codes: List of ISO 3-letter country codes
            start_year: Start year (optional)
            end_year: End year (optional)
        
        Returns:
            Dictionary mapping country codes to their CPI data
        """
        results = {}
        for country_code in country_codes:
            print(f"Fetching CPI data for {country_code}...")
            results[country_code] = self.get_cpi_data(country_code, start_year, end_year)
        return results
    
    def _parse_cpi_records(self, records: List[Dict]) -> List[Dict]:
        """
        Parse and normalize CPI records from WorldBank API response
        
        Args:
            records: Raw records from API
        
        Returns:
            List of normalized records
        """
        parsed = []
        for record in records:
            if record.get('value') is not None:
                parsed.append({
                    'country_code': record.get('countryiso3code', ''),
                    'country_name': record.get('country', {}).get('value', ''),
                    'year': int(record.get('date', 0)),
                    'cpi_total': float(record.get('value', 0)),
                    'indicator': record.get('indicator', {}).get('value', ''),
                    'indicator_id': record.get('indicator', {}).get('id', '')
                })
        return sorted(parsed, key=lambda x: x['year'])
    
    def get_all_countries_cpi(self, start_year: int = 1980, 
                              end_year: Optional[int] = None) -> Dict[str, List[Dict]]:
        """
        Fetch CPI data for all available countries
        
        Args:
            start_year: Start year (default: 1980 - base year)
            end_year: End year (optional, defaults to current year)
        
        Returns:
            Dictionary mapping country codes to their CPI data
        """
        # Use 'all' as country code to get all countries
        url = f"{self.BASE_URL}/country/all/indicator/{self.CPI_INDICATOR}"
        params = {
            'format': self.format,
            'per_page': self.per_page
        }
        
        if start_year:
            end = end_year or datetime.now().year
            params['date'] = f"{start_year}:{end}"
        
        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            # Group by country
            if len(data) >= 2 and isinstance(data[1], list):
                all_records = self._parse_cpi_records(data[1])
                
                # Group by country_code
                grouped = {}
                for record in all_records:
                    country_code = record['country_code']
                    if country_code not in grouped:
                        grouped[country_code] = []
                    grouped[country_code].append(record)
                
                # Sort each country's records by year
                for country_code in grouped:
                    grouped[country_code] = sorted(grouped[country_code], key=lambda x: x['year'])
                
                return grouped
            else:
                return {}
                
        except requests.exceptions.RequestException as e:
            print(f"Error fetching all countries data: {e}")
            return {}
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"Error parsing response: {e}")
            return {}


if __name__ == "__main__":
    # Test the scraper
    scraper = WorldBankCPIScraper()
    
    # Test with a few countries
    test_countries = ['USA', 'RUS', 'DEU', 'GBR', 'JPN']
    print("Testing WorldBank CPI Scraper")
    print("=" * 50)
    
    for country in test_countries:
        print(f"\nFetching data for {country}...")
        data = scraper.get_cpi_data(country, start_year=1980, end_year=2023)
        
        if data:
            print(f"  Found {len(data)} records")
            print(f"  Years range: {data[0]['year']} - {data[-1]['year']}")
            print(f"  Sample record (1980): {next((r for r in data if r['year'] == 1980), 'N/A')}")
            print(f"  Sample record (2020): {next((r for r in data if r['year'] == 2020), 'N/A')}")
        else:
            print(f"  No data found for {country}")

