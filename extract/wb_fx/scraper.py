"""
WorldBank Exchange Rate Scraper
Fetches official exchange rates (LCU per USD) from WorldBank API
Indicator: PA.NUS.FCRF
"""

import requests
import json
from typing import List, Dict, Optional
from datetime import datetime


class WorldBankFXScraper:
    """
    Scraper for WorldBank official exchange rate data (LCU per USD).
    API endpoint: https://api.worldbank.org/v2/country/all/indicator/PA.NUS.FCRF
    """

    BASE_URL = "https://api.worldbank.org/v2"
    FX_INDICATOR = "PA.NUS.FCRF"

    def __init__(self, format: str = "json", per_page: int = 20000):
        self.format = format
        self.per_page = per_page
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0'
        })

    def _parse_fx_records(self, records: List[Dict]) -> List[Dict]:
        parsed = []
        for record in records:
            if record.get('value') is not None:
                parsed.append({
                    'country_code': record.get('countryiso3code', ''),
                    'country_name': record.get('country', {}).get('value', ''),
                    'year': int(record.get('date', 0)),
                    'exchange_rate': float(record.get('value', 0)),
                })
        return sorted(parsed, key=lambda x: (x['country_code'], x['year']))

    def get_all_countries_fx(self, start_year: int = 1960,
                             end_year: Optional[int] = None) -> Dict[str, List[Dict]]:
        """
        Fetch exchange rate data for all countries.

        Returns:
            Dictionary mapping country codes to their exchange rate data
        """
        url = f"{self.BASE_URL}/country/all/indicator/{self.FX_INDICATOR}"
        end = end_year or datetime.now().year
        params = {
            'format': self.format,
            'per_page': self.per_page,
            'date': f"{start_year}:{end}",
        }

        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            if len(data) >= 2 and isinstance(data[1], list):
                all_records = self._parse_fx_records(data[1])
                grouped = {}
                for record in all_records:
                    cc = record['country_code']
                    if cc not in grouped:
                        grouped[cc] = []
                    grouped[cc].append(record)
                return grouped
            else:
                return {}

        except requests.exceptions.RequestException as e:
            print(f"Error fetching exchange rate data: {e}")
            return {}
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"Error parsing response: {e}")
            return {}


if __name__ == "__main__":
    scraper = WorldBankFXScraper()
    print("Testing WorldBank Exchange Rate Scraper")
    print("=" * 50)

    grouped = scraper.get_all_countries_fx(start_year=2010, end_year=2023)
    print(f"Countries with data: {len(grouped)}")

    for cc in ['USA', 'RUS', 'ARM', 'GBR']:
        records = grouped.get(cc, [])
        if records:
            print(f"\n{cc}: {len(records)} records")
            print(f"  {records[0]['year']}: {records[0]['exchange_rate']}")
            print(f"  {records[-1]['year']}: {records[-1]['exchange_rate']}")
        else:
            print(f"\n{cc}: no data")
