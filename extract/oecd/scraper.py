"""
OECD Average Annual Wage Scraper
Fetches average annual wage data from OECD SDMX-JSON API
Uses a single bulk request to avoid rate limiting.
"""

import requests
import json
import time
from typing import List, Dict, Optional
from datetime import datetime


class OECDWageScraper:
    """
    Scraper for OECD average annual wage data.
    Fetches all countries in one bulk API call and parses locally.
    Filters for UNIT_MEASURE=USD_PPP (US dollars, PPP converted).
    """

    BASE_URL = "https://stats.oecd.org/SDMX-JSON/data"
    DATASET = "AV_AN_WAGE"

    def __init__(self):
        """Initialize scraper"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0',
            'Accept': 'application/vnd.sdmx.data+json;version=1.0.0'
        })
        self._bulk_cache = None

    def _fetch_bulk(self) -> Dict[str, List[Dict]]:
        """
        Single bulk request for all countries, all years.
        Returns dict: country_code -> list of records.
        """
        if self._bulk_cache is not None:
            return self._bulk_cache

        url = f"{self.BASE_URL}/{self.DATASET}/all/all/all"

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=120)
                response.raise_for_status()
                data = response.json()
                self._bulk_cache = self._parse_bulk_response(data)
                return self._bulk_cache
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                print(f"Error fetching OECD bulk data: {e}")
                return {}
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                print(f"Error parsing OECD bulk response: {e}")
                return {}

        return {}

    def _parse_bulk_response(self, data: Dict) -> Dict[str, List[Dict]]:
        """
        Parse bulk SDMX-JSON response.
        Filters for UNIT_MEASURE=USD_PPP to get PPP-converted wages.
        Returns dict: country_code -> sorted list of records.
        """
        result = {}

        try:
            data_section = data.get('data', {})
            datasets = data_section.get('dataSets', [])
            if not datasets:
                return {}

            dataset = datasets[0]
            structure_ref = dataset.get('structure', 0)
            structures = data_section.get('structures', [])
            if structure_ref >= len(structures):
                return {}

            structure = structures[structure_ref]
            dimensions = structure.get('dimensions', {})

            # Time dimension
            obs_dimensions = dimensions.get('observation', [])
            if not obs_dimensions:
                return {}
            time_dim = obs_dimensions[0]
            time_values = {i: val.get('name', '') for i, val in enumerate(time_dim.get('values', []))}

            # Series dimensions
            series_dims = dimensions.get('series', [])
            ref_area_map = {}       # index -> {id, name}
            unit_measure_dim_pos = None
            unit_measure_ppp_index = None

            for dim_pos, dim in enumerate(series_dims):
                if dim.get('id') == 'REF_AREA':
                    for i, val in enumerate(dim.get('values', [])):
                        ref_area_map[i] = {
                            'id': val.get('id', ''),
                            'name': val.get('name', ''),
                        }
                elif dim.get('id') == 'UNIT_MEASURE':
                    unit_measure_dim_pos = dim_pos
                    for i, val in enumerate(dim.get('values', [])):
                        if val.get('id') == 'USD_PPP':
                            unit_measure_ppp_index = i
                            break

            # Parse all series
            series_dict = dataset.get('series', {})

            for series_key, series_data in series_dict.items():
                key_parts = [int(x) for x in series_key.split(':')]

                # Filter: UNIT_MEASURE == USD_PPP
                if unit_measure_dim_pos is not None and unit_measure_ppp_index is not None:
                    if key_parts[unit_measure_dim_pos] != unit_measure_ppp_index:
                        continue

                country_info = ref_area_map.get(key_parts[0])
                if not country_info:
                    continue

                country_code = country_info['id']
                country_name = country_info['name']

                observations = series_data.get('observations', {})
                for obs_key, value_list in observations.items():
                    obs_index = int(obs_key)
                    value = value_list[0] if isinstance(value_list, list) and len(value_list) > 0 else value_list

                    if value is not None:
                        time_period = time_values.get(obs_index, '')
                        try:
                            year = int(time_period) if time_period.isdigit() else None
                            if year:
                                result.setdefault(country_code, []).append({
                                    'country_code': country_code,
                                    'country_name': country_name,
                                    'year': year,
                                    'avg_annual_wage_usd': float(value),
                                    'currency': 'USD',
                                    'measure': 'Average'
                                })
                        except (ValueError, TypeError):
                            continue

            # Sort each country's records by year
            for cc in result:
                result[cc].sort(key=lambda x: x['year'])

            return result

        except (KeyError, ValueError, IndexError, TypeError) as e:
            print(f"Error parsing OECD bulk SDMX data: {e}")
            import traceback
            traceback.print_exc()
            return {}

    def get_wage_data(self, country_code: str) -> List[Dict]:
        """
        Get wage data for a specific country (from bulk cache).

        Args:
            country_code: ISO 3-letter country code (e.g., 'USA', 'DEU')

        Returns:
            List of dictionaries with wage data, sorted by year
        """
        bulk = self._fetch_bulk()
        return bulk.get(country_code, [])

    def get_wage_data_multiple_countries(self, country_codes: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch wage data for multiple countries

        Args:
            country_codes: List of ISO 3-letter country codes

        Returns:
            Dictionary mapping country codes to their wage data
        """
        bulk = self._fetch_bulk()
        return {cc: bulk.get(cc, []) for cc in country_codes}

    def get_all_oecd_countries_wages(self) -> Dict[str, List[Dict]]:
        """
        Fetch wage data for all OECD countries.
        Single bulk request, no per-country calls.

        Returns:
            Dictionary mapping country codes to their wage data
        """
        bulk = self._fetch_bulk()
        print(f"Found {len(bulk)} OECD countries")
        return bulk


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
