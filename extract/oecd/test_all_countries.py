"""
Test script to check all OECD countries and their data ranges
"""

from scraper import OECDWageScraper


def test_all_countries():
    scraper = OECDWageScraper()
    
    print("=" * 60)
    print("Fetching wage data for ALL OECD countries...")
    print("=" * 60)
    
    all_data = scraper.get_all_oecd_countries_wages()
    
    print(f"\nTotal countries: {len(all_data)}")
    countries_with_data = [c for c, d in all_data.items() if d]
    print(f"Countries with data: {len(countries_with_data)}")
    
    # Analyze year ranges
    print("\n" + "=" * 60)
    print("Year ranges by country:")
    print("=" * 60)
    
    for code in sorted(countries_with_data):
        data = all_data[code]
        if data:
            earliest = data[0]['year']
            latest = data[-1]['year']
            count = len(data)
            print(f"  {code:3s}: {count:3d} records, {earliest}-{latest}")
    
    # Check earliest years
    print("\n" + "=" * 60)
    print("Earliest available years:")
    print("=" * 60)
    earliest_years = {}
    for code, data in all_data.items():
        if data:
            year = data[0]['year']
            if year not in earliest_years:
                earliest_years[year] = []
            earliest_years[year].append(code)
    
    for year in sorted(earliest_years.keys()):
        countries = earliest_years[year]
        print(f"  {year}: {len(countries)} countries - {', '.join(countries[:10])}{'...' if len(countries) > 10 else ''}")


if __name__ == "__main__":
    test_all_countries()

