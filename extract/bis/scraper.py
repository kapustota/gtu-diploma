"""
BIS Housing Price Index Scraper
Fetches residential property price index data from BIS SDMX REST API
Data is quarterly index (2010=100), aggregated to annual average
"""

import requests
import pandas as pd
import io
import time
from typing import List, Dict, Optional
from datetime import datetime


# ISO2 → ISO3 country code mapping
ISO2_TO_ISO3 = {
    "AF": "AFG", "AL": "ALB", "DZ": "DZA", "AS": "ASM", "AD": "AND",
    "AO": "AGO", "AG": "ATG", "AR": "ARG", "AM": "ARM", "AU": "AUS",
    "AT": "AUT", "AZ": "AZE", "BS": "BHS", "BH": "BHR", "BD": "BGD",
    "BB": "BRB", "BY": "BLR", "BE": "BEL", "BZ": "BLZ", "BJ": "BEN",
    "BT": "BTN", "BO": "BOL", "BA": "BIH", "BW": "BWA", "BR": "BRA",
    "BN": "BRN", "BG": "BGR", "BF": "BFA", "BI": "BDI", "KH": "KHM",
    "CM": "CMR", "CA": "CAN", "CV": "CPV", "CF": "CAF", "TD": "TCD",
    "CL": "CHL", "CN": "CHN", "CO": "COL", "KM": "COM", "CG": "COG",
    "CD": "COD", "CR": "CRI", "CI": "CIV", "HR": "HRV", "CU": "CUB",
    "CY": "CYP", "CZ": "CZE", "DK": "DNK", "DJ": "DJI", "DM": "DMA",
    "DO": "DOM", "EC": "ECU", "EG": "EGY", "SV": "SLV", "GQ": "GNQ",
    "ER": "ERI", "EE": "EST", "ET": "ETH", "FJ": "FJI", "FI": "FIN",
    "FR": "FRA", "GA": "GAB", "GM": "GMB", "GE": "GEO", "DE": "DEU",
    "GH": "GHA", "GR": "GRC", "GD": "GRD", "GT": "GTM", "GN": "GIN",
    "GW": "GNB", "GY": "GUY", "HT": "HTI", "HN": "HND", "HK": "HKG",
    "HU": "HUN", "IS": "ISL", "IN": "IND", "ID": "IDN", "IR": "IRN",
    "IQ": "IRQ", "IE": "IRL", "IL": "ISR", "IT": "ITA", "JM": "JAM",
    "JP": "JPN", "JO": "JOR", "KZ": "KAZ", "KE": "KEN", "KI": "KIR",
    "KP": "PRK", "KR": "KOR", "KW": "KWT", "KG": "KGZ", "LA": "LAO",
    "LV": "LVA", "LB": "LBN", "LS": "LSO", "LR": "LBR", "LY": "LBY",
    "LI": "LIE", "LT": "LTU", "LU": "LUX", "MO": "MAC", "MK": "MKD",
    "MG": "MDG", "MW": "MWI", "MY": "MYS", "MV": "MDV", "ML": "MLI",
    "MT": "MLT", "MH": "MHL", "MR": "MRT", "MU": "MUS", "MX": "MEX",
    "FM": "FSM", "MD": "MDA", "MC": "MCO", "MN": "MNG", "ME": "MNE",
    "MA": "MAR", "MZ": "MOZ", "MM": "MMR", "NA": "NAM", "NR": "NRU",
    "NP": "NPL", "NL": "NLD", "NZ": "NZL", "NI": "NIC", "NE": "NER",
    "NG": "NGA", "NO": "NOR", "OM": "OMN", "PK": "PAK", "PW": "PLW",
    "PA": "PAN", "PG": "PNG", "PY": "PRY", "PE": "PER", "PH": "PHL",
    "PL": "POL", "PT": "PRT", "QA": "QAT", "RO": "ROU", "RU": "RUS",
    "RW": "RWA", "KN": "KNA", "LC": "LCA", "VC": "VCT", "WS": "WSM",
    "SM": "SMR", "ST": "STP", "SA": "SAU", "SN": "SEN", "RS": "SRB",
    "SC": "SYC", "SL": "SLE", "SG": "SGP", "SK": "SVK", "SI": "SVN",
    "SB": "SLB", "SO": "SOM", "ZA": "ZAF", "ES": "ESP", "LK": "LKA",
    "SD": "SDN", "SR": "SUR", "SZ": "SWZ", "SE": "SWE", "CH": "CHE",
    "SY": "SYR", "TW": "TWN", "TJ": "TJK", "TZ": "TZA", "TH": "THA",
    "TL": "TLS", "TG": "TGO", "TO": "TON", "TT": "TTO", "TN": "TUN",
    "TR": "TUR", "TM": "TKM", "TV": "TUV", "UG": "UGA", "UA": "UKR",
    "AE": "ARE", "GB": "GBR", "US": "USA", "UY": "URY", "UZ": "UZB",
    "VU": "VUT", "VE": "VEN", "VN": "VNM", "YE": "YEM", "ZM": "ZMB",
    "ZW": "ZWE", "XM": "XM",
}

# ISO2 → country name mapping (for common BIS countries)
ISO2_TO_NAME = {
    "US": "United States", "GB": "United Kingdom", "DE": "Germany",
    "FR": "France", "JP": "Japan", "CN": "China", "CA": "Canada",
    "AU": "Australia", "IT": "Italy", "ES": "Spain", "NL": "Netherlands",
    "CH": "Switzerland", "SE": "Sweden", "NO": "Norway", "DK": "Denmark",
    "BE": "Belgium", "AT": "Austria", "PL": "Poland", "KR": "South Korea",
    "IN": "India", "BR": "Brazil", "MX": "Mexico", "ZA": "South Africa",
    "TR": "Turkey", "RU": "Russia", "AR": "Argentina", "CL": "Chile",
    "CO": "Colombia", "PE": "Peru", "ID": "Indonesia", "MY": "Malaysia",
    "TH": "Thailand", "PH": "Philippines", "SG": "Singapore",
    "HK": "Hong Kong", "NZ": "New Zealand", "IE": "Ireland",
    "PT": "Portugal", "GR": "Greece", "FI": "Finland", "CZ": "Czech Republic",
    "HU": "Hungary", "RO": "Romania", "BG": "Bulgaria", "HR": "Croatia",
    "SK": "Slovakia", "SI": "Slovenia", "EE": "Estonia", "LV": "Latvia",
    "LT": "Lithuania", "IL": "Israel", "SA": "Saudi Arabia",
    "AE": "United Arab Emirates", "LU": "Luxembourg", "IS": "Iceland",
    "MT": "Malta", "CY": "Cyprus",
}


class BISHousingPriceScraper:
    """
    Scraper for BIS residential property price index (2010=100)
    Uses BIS SDMX REST API: stats.bis.org/api/v1/data/BIS,WS_SPP,1.0/...
    Quarterly data aggregated to annual average
    """

    API_URL = "https://stats.bis.org/api/v1/data/BIS,WS_SPP,1.0/Q..N.628"

    def __init__(self):
        """Initialize scraper"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ETL-Pipeline/1.0'
        })
        self._data_cache = None

    def _fetch_csv_data(self) -> pd.DataFrame:
        """
        Fetch data from BIS SDMX REST API in CSV format

        Returns:
            DataFrame with raw quarterly data
        """
        if self._data_cache is not None:
            return self._data_cache

        max_retries = 3
        retry_delay = 2

        for attempt in range(max_retries):
            try:
                print(f"Fetching BIS housing price index data from SDMX API...")
                response = self.session.get(
                    self.API_URL,
                    params={"format": "csv"},
                    timeout=120
                )

                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (attempt + 1)
                        print(f"Rate limited, waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception("Rate limited after multiple attempts")

                response.raise_for_status()

                df = pd.read_csv(io.StringIO(response.text))
                self._data_cache = df
                print(f"Fetched {len(df)} quarterly records from BIS API")
                return df

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                raise Exception(f"Failed to fetch BIS data: {e}")

        raise Exception("Failed to fetch BIS data after multiple attempts")

    def _aggregate_to_annual(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate quarterly data to annual averages

        Args:
            df: DataFrame with quarterly data (TIME_PERIOD like 2020-Q1)

        Returns:
            DataFrame with annual averages
        """
        # Extract year from TIME_PERIOD (format: 2020-Q1)
        df = df.copy()
        df['year'] = df['TIME_PERIOD'].str.extract(r'(\d{4})').astype(int)

        # Group by REF_AREA and year, compute annual average
        annual = df.groupby(['REF_AREA', 'year']).agg(
            OBS_VALUE=('OBS_VALUE', 'mean')
        ).reset_index()

        return annual

    def get_price_data(self, country_code: Optional[str] = None) -> List[Dict]:
        """
        Fetch housing price index data

        Args:
            country_code: ISO 3-letter country code (optional, None = all countries)

        Returns:
            List of dictionaries with housing price index data
        """
        try:
            df = self._fetch_csv_data()

            # Filter to rows with valid OBS_VALUE
            df = df[pd.notna(df['OBS_VALUE'])].copy()

            # Aggregate quarterly → annual
            annual = self._aggregate_to_annual(df)

            parsed = []
            for _, row in annual.iterrows():
                iso2 = str(row['REF_AREA']).strip()
                iso3 = ISO2_TO_ISO3.get(iso2)
                if not iso3:
                    continue

                # Filter by country if specified
                if country_code and iso3 != country_code.upper():
                    continue

                year = int(row['year'])
                if year < 1980 or year > datetime.now().year + 1:
                    continue

                country_name = ISO2_TO_NAME.get(iso2, iso3)

                parsed.append({
                    'country_code': iso3,
                    'country_name': country_name,
                    'year': year,
                    'housing_price_index': round(float(row['OBS_VALUE']), 2),
                    'indicator': 'Residential property price index (2010=100)',
                    'source': 'BIS'
                })

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
        # Fetch all data once, then filter
        self._fetch_csv_data()

        results = {}
        for country_code in country_codes:
            print(f"Filtering price data for {country_code}...")
            results[country_code] = self.get_price_data(country_code)

        return results

    def get_all_countries_prices(self) -> Dict[str, List[Dict]]:
        """
        Fetch price data for all available countries

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

        for country_code in grouped:
            grouped[country_code] = sorted(grouped[country_code], key=lambda x: x['year'])

        print(f"Found data for {len(grouped)} countries")
        return grouped


if __name__ == "__main__":
    scraper = BISHousingPriceScraper()

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
