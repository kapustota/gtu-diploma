"""
ILOSTAT Gross Wages Scraper
Fetches gross wages data from ILOSTAT SDMX API
Uses DF_EAR_4HRL_SEX_CUR_NB dataflow (Average hourly earnings)
ILOSTAT is used as a backup source for countries not in OECD.
"""

import requests
import json
import time
from typing import List, Dict, Optional
from datetime import datetime


class ILOSTATWageScraper:
    """
    Scraper for ILOSTAT gross wages data
    Uses DF_EAR_4HRL_SEX_CUR_NB - Average hourly earnings of employees
    ILOSTAT is used as a backup source for countries not in OECD.
    """
    
    BASE_URL = "https://sdmx.ilo.org/rest/data"
    DATASET = "ILO,DF_EAR_4HRL_SEX_CUR_NB,1.0"  # Average hourly earnings
    
    def __init__(self):
        """Initialize scraper"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0',
            'Accept': 'application/vnd.sdmx.data+json'
        })
        self._data_cache = None
    
    def get_wage_data(self, country_code: str) -> List[Dict]:
        """
        Fetch gross wage data for a specific country
        Gets all available years from the earliest available
        
        Args:
            country_code: ISO 3-letter country code (e.g., 'USA', 'RUS')
        
        Returns:
            List of dictionaries with wage data
        """
        # Fetch all data and filter by country
        if self._data_cache is None:
            self._data_cache = self._fetch_all_data()
        
        # Filter by country
        filtered = [r for r in self._data_cache if r.get('country_code') == country_code.upper()]
        return sorted(filtered, key=lambda x: x['year'])
    
    def _fetch_all_data(self) -> List[Dict]:
        """
        Fetch all wage data from ILOSTAT
        
        Returns:
            List of all wage records
        """
        url = f"{self.BASE_URL}/{self.DATASET}/all"
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                print("Fetching ILOSTAT wages data...")
                response = self.session.get(url, timeout=120)
                
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        print(f"Rate limited, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print("Rate limited after multiple attempts")
                        return []
                
                response.raise_for_status()
                data = response.json()
                return self._parse_sdmx_response(data)
                    
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                print(f"Error fetching data: {e}")
                return []
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Error parsing response: {e}")
                return []
        
        return []
    
    def _parse_sdmx_response(self, data: Dict) -> List[Dict]:
        """
        Parse SDMX-JSON response from ILOSTAT API.
        Filters for CUR_TYPE_USD (US dollars) and SEX_T (total) only
        to avoid mixing local-currency values with USD.

        Args:
            data: Raw JSON response from API

        Returns:
            List of normalized records
        """
        parsed = []

        try:
            # SDMX-JSON structure similar to OECD
            data_section = data.get('data', {})
            datasets = data_section.get('dataSets', [])

            if not datasets:
                return []

            dataset = datasets[0]
            structure_ref = dataset.get('structure', 0)

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
            cur_dim_pos = None
            cur_usd_index = None
            sex_dim_pos = None
            sex_total_index = None

            for dim_pos, dim in enumerate(series_dims):
                if dim.get('id') == 'REF_AREA':
                    ref_area_dim = dim
                elif dim.get('id') == 'CUR':
                    cur_dim_pos = dim_pos
                    for i, val in enumerate(dim.get('values', [])):
                        if val.get('id') == 'CUR_TYPE_USD':
                            cur_usd_index = i
                            break
                elif dim.get('id') == 'SEX':
                    sex_dim_pos = dim_pos
                    for i, val in enumerate(dim.get('values', [])):
                        if val.get('id') == 'SEX_T':
                            sex_total_index = i
                            break

            if not ref_area_dim:
                return []

            # Create mapping of dimension indices to country codes
            country_map = {}
            for i, val in enumerate(ref_area_dim.get('values', [])):
                country_code = val.get('id', '')
                country_name = val.get('name', '')
                country_map[i] = {'code': country_code, 'name': country_name}

            # Parse series data
            series_dict = dataset.get('series', {})

            for series_key, series_data in series_dict.items():
                # Parse series key (format: "1:0:0:0:0" where first is REF_AREA index)
                key_parts = [int(x) for x in series_key.split(':')]

                # Filter by CUR=USD
                if cur_dim_pos is not None and cur_usd_index is not None:
                    if key_parts[cur_dim_pos] != cur_usd_index:
                        continue

                # Filter by SEX=Total
                if sex_dim_pos is not None and sex_total_index is not None:
                    if key_parts[sex_dim_pos] != sex_total_index:
                        continue

                # Get country from first dimension
                country_info = country_map.get(key_parts[0])
                if not country_info:
                    continue

                country_code = country_info['code']
                country_name = country_info['name']

                observations = series_data.get('observations', {})

                for obs_key, value_list in observations.items():
                    obs_index = int(obs_key)
                    # Value is first element of the list
                    value = value_list[0] if isinstance(value_list, list) and len(value_list) > 0 else value_list

                    if value is not None:
                        # Get time period from time dimension
                        time_period = time_values.get(obs_index, '')
                        try:
                            year = int(time_period) if time_period.isdigit() else None

                            if year:
                                parsed.append({
                                    'country_code': country_code,
                                    'country_name': country_name,
                                    'year': year,
                                    'gross_wage': float(value),
                                    'indicator': 'Average hourly earnings',
                                    'currency': 'USD',
                                    'source': 'ILOSTAT'
                                })
                        except (ValueError, TypeError):
                            continue

            print(f"Parsed {len(parsed)} records from ILOSTAT")
            return parsed
            
        except (KeyError, ValueError, IndexError, TypeError) as e:
            print(f"Error parsing SDMX data: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_wage_data_multiple_countries(self, country_codes: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch wage data for multiple countries
        
        Args:
            country_codes: List of ISO 3-letter country codes
        
        Returns:
            Dictionary mapping country codes to their wage data
        """
        # Fetch all data once
        if self._data_cache is None:
            self._data_cache = self._fetch_all_data()
        
        results = {}
        for country_code in country_codes:
            print(f"Filtering wage data for {country_code}...")
            results[country_code] = self.get_wage_data(country_code)
        
        return results
    
    def get_all_countries_wages(self) -> Dict[str, List[Dict]]:
        """
        Fetch wage data for all available countries
        Gets all available years from the earliest available for each country
        
        Returns:
            Dictionary mapping country codes to their wage data
        """
        print("Fetching wage data for all countries...")
        all_data = self._fetch_all_data()
        
        # Group by country
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
    scraper = ILOSTATWageScraper()
    
    # Test with a few countries
    test_countries = ['USA', 'RUS', 'DEU', 'GBR', 'JPN']
    print("Testing ILOSTAT Wage Scraper")
    print("=" * 50)
    
    for country in test_countries:
        print(f"\nFetching data for {country}...")
        data = scraper.get_wage_data(country)
        
        if data:
            print(f"  Found {len(data)} records")
            print(f"  Years range: {data[0]['year']} - {data[-1]['year']}")
            print(f"  Sample record (first): {data[0]}")
            if len(data) > 1:
                print(f"  Sample record (last): {data[-1]}")
        else:
            print(f"  No data found for {country}")
