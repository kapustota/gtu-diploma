"""
ILOSTAT Average Monthly Wages Scraper
Fetches nominal average monthly wages in local currency from ILOSTAT bulk CSV API.
Uses EAR_4MTH_SEX_CUR_NB indicator (Average monthly earnings of employees).
Filters: sex=SEX_T (total), classif1=CUR_TYPE_LCU (local currency).
Coverage: ~191 countries, 115 with 10+ year time series.
"""

import csv
import io
import time
from typing import List, Dict
from datetime import datetime

import requests


class ILOSTATWageScraper:
    """
    Scraper for ILOSTAT average monthly wages (local currency).
    Uses the rplumber bulk CSV API which has far better coverage
    than the SDMX API (full time series vs 1-2 data points).
    """

    CSV_URL = (
        "https://rplumber.ilo.org/data/indicator/"
        "?id=EAR_4MTH_SEX_CUR_NB_A&type=code&format=csv"
    )

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0',
        })
        self._data_cache = None

    def _fetch_all_data(self) -> List[Dict]:
        """
        Download bulk CSV from ILOSTAT rplumber API.
        Filters for SEX_T + CUR_TYPE_LCU in-memory.
        """
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                print("Fetching ILOSTAT monthly wages CSV (~2.5MB)...")
                response = self.session.get(self.CSV_URL, timeout=120)

                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        print(f"Rate limited, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print("Rate limited after multiple attempts")
                        return []

                response.raise_for_status()
                return self._parse_csv(response.text)

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                print(f"Error fetching data: {e}")
                return []

        return []

    def _parse_csv(self, text: str) -> List[Dict]:
        """
        Parse CSV response. Columns (type=code):
        ref_area, source, indicator, sex, classif1, time, obs_value,
        obs_status, note_classif, note_indicator, note_source
        """
        # Handle BOM
        if text.startswith('\ufeff'):
            text = text[1:]

        reader = csv.DictReader(io.StringIO(text))
        parsed = []

        for row in reader:
            # Filter: total sex, local currency only
            if row.get('sex') != 'SEX_T':
                continue
            if row.get('classif1') != 'CUR_TYPE_LCU':
                continue

            country_code = row.get('ref_area', '').strip()
            year_str = row.get('time', '').strip()
            value_str = row.get('obs_value', '').strip()

            if not country_code or not year_str or not value_str:
                continue

            try:
                year = int(year_str)
                value = float(value_str)
            except (ValueError, TypeError):
                continue

            parsed.append({
                'country_code': country_code,
                'country_name': '',
                'year': year,
                'gross_wage': value,
                'indicator': 'Average monthly earnings',
                'currency': 'LCU',
                'source': 'ILOSTAT',
            })

        print(f"Parsed {len(parsed)} wage records from ILOSTAT")
        return parsed

    def get_wage_data(self, country_code: str) -> List[Dict]:
        """
        Fetch gross wage data for a specific country.

        Args:
            country_code: ISO 3-letter country code (e.g., 'USA', 'RUS')

        Returns:
            List of dictionaries with wage data
        """
        if self._data_cache is None:
            self._data_cache = self._fetch_all_data()

        filtered = [r for r in self._data_cache if r['country_code'] == country_code.upper()]
        return sorted(filtered, key=lambda x: x['year'])

    def get_wage_data_multiple_countries(self, country_codes: List[str]) -> Dict[str, List[Dict]]:
        """
        Fetch wage data for multiple countries.

        Args:
            country_codes: List of ISO 3-letter country codes

        Returns:
            Dictionary mapping country codes to their wage data
        """
        if self._data_cache is None:
            self._data_cache = self._fetch_all_data()

        results = {}
        for country_code in country_codes:
            results[country_code] = self.get_wage_data(country_code)

        return results

    def get_all_countries_wages(self) -> Dict[str, List[Dict]]:
        """
        Fetch wage data for all available countries.

        Returns:
            Dictionary mapping country codes to their wage data
        """
        print("Fetching wage data for all countries...")
        all_data = self._fetch_all_data()

        grouped = {}
        for record in all_data:
            cc = record['country_code']
            if cc not in grouped:
                grouped[cc] = []
            grouped[cc].append(record)

        for cc in grouped:
            grouped[cc] = sorted(grouped[cc], key=lambda x: x['year'])

        print(f"Found data for {len(grouped)} countries")
        return grouped


if __name__ == "__main__":
    scraper = ILOSTATWageScraper()

    test_countries = ['USA', 'RUS', 'DEU', 'GBR', 'JPN', 'KOR', 'IND', 'BRA']
    print("Testing ILOSTAT Wage Scraper (rplumber CSV API)")
    print("=" * 50)

    for country in test_countries:
        print(f"\nFetching data for {country}...")
        data = scraper.get_wage_data(country)

        if data:
            print(f"  Found {len(data)} records")
            print(f"  Years range: {data[0]['year']} - {data[-1]['year']}")
            print(f"  Sample (first): {data[0]['year']}: {data[0]['gross_wage']}")
            print(f"  Sample (last):  {data[-1]['year']}: {data[-1]['gross_wage']}")
        else:
            print(f"  No data found for {country}")
