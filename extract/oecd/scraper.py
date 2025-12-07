"""
OECD Average Annual Wage Scraper
Fetches average annual wage data from OECD SDMX-JSON API
"""

import requests
import json
import time
from typing import List, Dict, Optional
from datetime import datetime


class OECDWageScraper:
    """
    Scraper for OECD average annual wage data
    API endpoint: https://stats.oecd.org/SDMX-JSON/data/AV_AN_WAGE/{country_code}..USD.A/all
    """
    
    BASE_URL = "https://stats.oecd.org/SDMX-JSON/data"
    DATASET = "AV_AN_WAGE"
    CURRENCY = "USD"
    MEASURE = "A"  # Average
    
    def __init__(self):
        """Initialize scraper"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0',
            'Accept': 'application/vnd.sdmx.data+json;version=1.0.0'
        })
    
    def get_wage_data(self, country_code: str) -> List[Dict]:
        """
        Fetch average annual wage data for a specific country
        Gets all available years from the earliest available
        
        Args:
            country_code: ISO 3-letter country code (e.g., 'USA', 'DEU')
        
        Returns:
            List of dictionaries with wage data, sorted by year
        """
        # OECD SDMX format: AV_AN_WAGE/{country_code}..USD.A/all
        # Format: dataset/country..currency.measure/all
        # Don't filter by time to get all available years from the earliest
        url = f"{self.BASE_URL}/{self.DATASET}/{country_code}..{self.CURRENCY}.{self.MEASURE}/all"
        
        params = {}
        # Note: We don't filter by start_year/end_year to get all available data
        # Spark will handle filtering if needed
        
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                # Handle rate limiting
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        print(f"Rate limited for {country_code}, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"Rate limited for {country_code} after {max_retries} attempts")
                        return []
                
                response.raise_for_status()
                data = response.json()
                return self._parse_sdmx_response(data, country_code)
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                print(f"Error fetching data for {country_code}: {e}")
                return []
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Error parsing response for {country_code}: {e}")
                return []
        
        return []
    
    def get_wage_data_multiple_countries(self, country_codes: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch wage data for multiple countries
        Gets all available years from the earliest available for each country
        
        Args:
            country_codes: List of ISO 3-letter country codes
        
        Returns:
            Dictionary mapping country codes to their wage data
        """
        results = {}
        for country_code in country_codes:
            print(f"Fetching wage data for {country_code}...")
            results[country_code] = self.get_wage_data(country_code)
        return results
    
    def _parse_sdmx_response(self, data: Dict, country_code: str) -> List[Dict]:
        """
        Parse SDMX-JSON response from OECD API
        
        Args:
            data: Raw JSON response from API
            country_code: Country code for fallback
        
        Returns:
            List of normalized records
        """
        parsed = []
        
        try:
            # SDMX-JSON structure: data.dataSets[0].series -> {key: {observations: {...}}}
            # data.structures[structure_ref].dimensions.observation[0].values -> time periods
            data_section = data.get('data', {})
            datasets = data_section.get('dataSets', [])
            
            if not datasets:
                return []
            
            # Get structure reference from dataset
            dataset = datasets[0]
            structure_ref = dataset.get('structure', 0)
            
            # Get structure from structures array
            structures = data_section.get('structures', [])
            if structure_ref >= len(structures):
                return []
            
            structure = structures[structure_ref]
            dimensions = structure.get('dimensions', {})
            
            # Get time dimension
            obs_dimensions = dimensions.get('observation', [])
            if not obs_dimensions:
                return []
            
            time_dim = obs_dimensions[0]
            time_values = {i: val.get('name', '') for i, val in enumerate(time_dim.get('values', []))}
            
            # Get series dimensions to parse series keys
            series_dims = dimensions.get('series', [])
            ref_area_dim = None
            country_index = None
            for dim in series_dims:
                if dim.get('id') == 'REF_AREA':
                    ref_area_dim = dim
                    # Find index of requested country
                    for i, val in enumerate(dim.get('values', [])):
                        if val.get('id') == country_code:
                            country_index = i
                            break
                    break
            
            if country_index is None:
                print(f"Country {country_code} not found in REF_AREA dimension")
                return []
            
            # Parse series data - only for the requested country
            series_dict = dataset.get('series', {})
            
            for series_key, series_data in series_dict.items():
                # Parse series key (format: "1:0:0:0:0:0:0" where first is REF_AREA index)
                key_parts = [int(x) for x in series_key.split(':')]
                
                # Filter by country: first element of key is REF_AREA index
                if key_parts[0] != country_index:
                    continue  # Skip series for other countries
                
                observations = series_data.get('observations', {})
                
                for obs_key, value_list in observations.items():
                    obs_index = int(obs_key)
                    value = value_list[0] if isinstance(value_list, list) and len(value_list) > 0 else value_list
                    
                    if value is not None:
                        # Get time period from time dimension
                        time_period = time_values.get(obs_index, '')
                        try:
                            year = int(time_period) if time_period.isdigit() else None
                            
                            if year:
                                parsed.append({
                                    'country_code': country_code,
                                    'country_name': self._get_country_name(country_code, data),
                                    'year': year,
                                    'avg_annual_wage_usd': float(value),
                                    'currency': 'USD',
                                    'measure': 'Average'
                                })
                        except (ValueError, TypeError):
                            continue
            
            return sorted(parsed, key=lambda x: x['year'])
            
        except (KeyError, ValueError, IndexError, TypeError) as e:
            print(f"Error parsing SDMX data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _parse_sdmx_alternative(self, data: Dict, country_code: str) -> List[Dict]:
        """
        Alternative parsing method for SDMX-JSON
        """
        parsed = []
        
        try:
            # Try to extract data from a simpler structure
            datasets = data.get('dataSets', [])
            if not datasets:
                return []
            
            # Look for observation arrays
            for dataset in datasets:
                series = dataset.get('series', {})
                for series_key, series_data in series.items():
                    observations = series_data.get('observations', {})
                    
                    # Try to get time dimension from structure
                    structure = data.get('structure', {})
                    time_dim = structure.get('dimensions', {}).get('observation', [])
                    
                    if time_dim:
                        time_values = time_dim[0].get('values', [])
                        for obs_key, value_list in observations.items():
                            obs_index = int(obs_key)
                            value = value_list[0] if isinstance(value_list, list) else value_list
                            
                            if value is not None and obs_index < len(time_values):
                                time_period = time_values[obs_index].get('name', '')
                                year = int(time_period) if time_period.isdigit() else None
                                
                                if year:
                                    parsed.append({
                                        'country_code': country_code,
                                        'country_name': self._get_country_name(country_code, data),
                                        'year': year,
                                        'avg_annual_wage_usd': float(value),
                                        'currency': 'USD',
                                        'measure': 'Average'
                                    })
            
            return sorted(parsed, key=lambda x: x['year'])
            
        except Exception as e:
            print(f"Alternative parsing also failed: {e}")
            return []
    
    def _get_country_name(self, country_code: str, data: Dict) -> str:
        """Extract country name from SDMX structure"""
        try:
            data_section = data.get('data', {})
            structures = data_section.get('structures', [])
            if not structures:
                return country_code
            
            structure = structures[0]
            dimensions = structure.get('dimensions', {}).get('series', [])
            
            for dim in dimensions:
                if dim.get('id') == 'LOCATION' or dim.get('id') == 'REF_AREA':
                    values = dim.get('values', [])
                    for val in values:
                        if val.get('id') == country_code:
                            return val.get('name', country_code)
        except:
            pass
        
        return country_code
    
    def get_all_oecd_countries_wages(self) -> Dict[str, List[Dict]]:
        """
        Fetch wage data for all OECD countries
        Gets all available years from the earliest available for each country
        
        Returns:
            Dictionary mapping country codes to their wage data
        """
        # Fetch all countries data to get the list
        url = f"{self.BASE_URL}/{self.DATASET}/all/all/all"
        
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            # Extract country list from structure
            data_section = data.get('data', {})
            structures = data_section.get('structures', [])
            if not structures:
                return {}
            
            structure = structures[0]
            series_dims = structure.get('dimensions', {}).get('series', [])
            ref_area_dim = None
            for dim in series_dims:
                if dim.get('id') == 'REF_AREA':
                    ref_area_dim = dim
                    break
            
            if not ref_area_dim:
                return {}
            
            # Get all country codes
            oecd_countries = [val.get('id') for val in ref_area_dim.get('values', [])]
            print(f"Found {len(oecd_countries)} OECD countries")
            
            # Fetch data for each country with rate limiting
            results = {}
            for i, country_code in enumerate(oecd_countries):
                print(f"Fetching wage data for {country_code} ({i+1}/{len(oecd_countries)})...")
                results[country_code] = self.get_wage_data(country_code)
                # Add delay to avoid rate limiting (429 errors)
                if i < len(oecd_countries) - 1:  # Don't delay after last request
                    time.sleep(1)  # 1 second delay between requests
            
            return results
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching all countries list: {e}")
            return {}
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"Error parsing all countries response: {e}")
            return {}


if __name__ == "__main__":
    # Test the scraper
    scraper = OECDWageScraper()
    
    # Test with a few countries
    test_countries = ['USA', 'DEU', 'GBR', 'JPN', 'FRA']
    print("Testing OECD Wage Scraper")
    print("=" * 50)
    
    for country in test_countries:
        print(f"\nFetching data for {country}...")
        data = scraper.get_wage_data(country)
        
        if data:
            print(f"  Found {len(data)} records")
            print(f"  Years range: {data[0]['year']} - {data[-1]['year']}")
            print(f"  Sample record (1980): {next((r for r in data if r['year'] == 1980), 'N/A')}")
            print(f"  Sample record (2020): {next((r for r in data if r['year'] == 2020), 'N/A')}")
        else:
            print(f"  No data found for {country}")

