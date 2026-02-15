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

    Fetches two datasets and merges them:
    - EAR_4MTH_SEX_CUR_NB_A: primary (sex=SEX_T, classif1=CUR_TYPE_LCU)
    - EAR_4MTH_SEX_ECO_CUR_NB_A: by economic sector, filtered to
      ECO_SECTOR_TOTAL + CUR_TYPE_LCU for supplementary coverage
    """

    CSV_URL = (
        "https://rplumber.ilo.org/data/indicator/"
        "?id=EAR_4MTH_SEX_CUR_NB_A&type=code&format=csv"
    )
    CSV_URL_ECO = (
        "https://rplumber.ilo.org/data/indicator/"
        "?id=EAR_4MTH_SEX_ECO_CUR_NB_A&type=code&format=csv"
    )

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0',
        })
        self._data_cache = None

    def _fetch_csv(self, url: str, label: str) -> str:
        """Download a CSV from ILOSTAT with retries. Returns raw text."""
        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                print(f"Fetching {label}...")
                response = self.session.get(url, timeout=120)

                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        print(f"Rate limited, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print("Rate limited after multiple attempts")
                        return ""

                response.raise_for_status()
                return response.text

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                print(f"Error fetching {label}: {e}")
                return ""

        return ""

    def _fetch_all_data(self) -> List[Dict]:
        """
        Download both primary and ECO CSVs, parse and merge.
        """
        primary = self._parse_primary_csv(
            self._fetch_csv(self.CSV_URL, "ILOSTAT primary wages CSV (~2.5MB)")
        )
        eco = self._parse_eco_csv(
            self._fetch_csv(self.CSV_URL_ECO, "ILOSTAT ECO sector wages CSV (~25MB)")
        )
        merged = primary + eco
        print(f"Total wage records: {len(merged)} (primary={len(primary)}, eco={len(eco)})")
        return merged

    def _parse_primary_csv(self, text: str) -> List[Dict]:
        """
        Parse primary CSV (EAR_4MTH_SEX_CUR_NB_A).
        Filters: sex=SEX_T, classif1=CUR_TYPE_LCU.
        """
        if not text:
            return []
        if text.startswith('\ufeff'):
            text = text[1:]

        reader = csv.DictReader(io.StringIO(text))
        parsed = []

        for row in reader:
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

        print(f"  Primary: {len(parsed)} records")
        return parsed

    def _parse_eco_csv(self, text: str) -> List[Dict]:
        """
        Parse ECO CSV (EAR_4MTH_SEX_ECO_CUR_NB_A).
        Filters: classif1=ECO_SECTOR_TOTAL, classif2=CUR_TYPE_LCU.
        Prefers sex=SEX_T; falls back to avg(SEX_M, SEX_F) when SEX_T is absent.
        """
        if not text:
            return []
        if text.startswith('\ufeff'):
            text = text[1:]

        reader = csv.DictReader(io.StringIO(text))

        # Collect by (country, year) -> {sex: [values]}
        by_pair: Dict[tuple, Dict[str, List[float]]] = {}
        for row in reader:
            if row.get('classif1') != 'ECO_SECTOR_TOTAL':
                continue
            if row.get('classif2') != 'CUR_TYPE_LCU':
                continue
            sex = row.get('sex', '')
            if sex not in ('SEX_T', 'SEX_M', 'SEX_F'):
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

            by_pair.setdefault((country_code, year), {}).setdefault(sex, []).append(value)

        parsed = []
        fallback_count = 0
        for (country_code, year), sexes in by_pair.items():
            if 'SEX_T' in sexes:
                values = sexes['SEX_T']
            elif 'SEX_M' in sexes and 'SEX_F' in sexes:
                values = [
                    (sum(sexes['SEX_M']) / len(sexes['SEX_M'])
                     + sum(sexes['SEX_F']) / len(sexes['SEX_F'])) / 2
                ]
                fallback_count += 1
            else:
                continue

            avg_value = sum(values) / len(values)
            parsed.append({
                'country_code': country_code,
                'country_name': '',
                'year': year,
                'gross_wage': avg_value,
                'indicator': 'Average monthly earnings',
                'currency': 'LCU',
                'source': 'ILOSTAT',
            })

        print(f"  ECO (SECTOR_TOTAL): {len(parsed)} records ({fallback_count} from avg M+F fallback)")
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
