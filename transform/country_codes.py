"""
Country code mappings for the transform pipeline
M49 (numeric) → ISO3 and ISO2 → ISO3
"""

# M49 numeric → ISO3 alpha-3 mapping
# Source: UN Statistics Division
M49_TO_ISO3 = {
    "4": "AFG", "8": "ALB", "12": "DZA", "16": "ASM", "20": "AND",
    "24": "AGO", "28": "ATG", "32": "ARG", "51": "ARM", "36": "AUS",
    "40": "AUT", "31": "AZE", "44": "BHS", "48": "BHR", "50": "BGD",
    "52": "BRB", "112": "BLR", "56": "BEL", "84": "BLZ", "204": "BEN",
    "64": "BTN", "68": "BOL", "70": "BIH", "72": "BWA", "76": "BRA",
    "96": "BRN", "100": "BGR", "854": "BFA", "108": "BDI", "116": "KHM",
    "120": "CMR", "124": "CAN", "132": "CPV", "140": "CAF", "148": "TCD",
    "152": "CHL", "156": "CHN", "170": "COL", "174": "COM", "178": "COG",
    "180": "COD", "188": "CRI", "384": "CIV", "191": "HRV", "192": "CUB",
    "196": "CYP", "203": "CZE", "208": "DNK", "262": "DJI", "212": "DMA",
    "214": "DOM", "218": "ECU", "818": "EGY", "222": "SLV", "226": "GNQ",
    "232": "ERI", "233": "EST", "231": "ETH", "242": "FJI", "246": "FIN",
    "250": "FRA", "266": "GAB", "270": "GMB", "268": "GEO", "276": "DEU",
    "288": "GHA", "300": "GRC", "308": "GRD", "320": "GTM", "324": "GIN",
    "624": "GNB", "328": "GUY", "332": "HTI", "340": "HND", "344": "HKG",
    "348": "HUN", "352": "ISL", "356": "IND", "360": "IDN", "364": "IRN",
    "368": "IRQ", "372": "IRL", "376": "ISR", "380": "ITA", "388": "JAM",
    "392": "JPN", "400": "JOR", "398": "KAZ", "404": "KEN", "296": "KIR",
    "408": "PRK", "410": "KOR", "414": "KWT", "417": "KGZ", "418": "LAO",
    "428": "LVA", "422": "LBN", "426": "LSO", "430": "LBR", "434": "LBY",
    "438": "LIE", "440": "LTU", "442": "LUX", "446": "MAC", "807": "MKD",
    "450": "MDG", "454": "MWI", "458": "MYS", "462": "MDV", "466": "MLI",
    "470": "MLT", "584": "MHL", "478": "MRT", "480": "MUS", "484": "MEX",
    "583": "FSM", "498": "MDA", "492": "MCO", "496": "MNG", "499": "MNE",
    "504": "MAR", "508": "MOZ", "104": "MMR", "516": "NAM", "520": "NRU",
    "524": "NPL", "528": "NLD", "554": "NZL", "558": "NIC", "562": "NER",
    "566": "NGA", "578": "NOR", "512": "OMN", "586": "PAK", "585": "PLW",
    "591": "PAN", "598": "PNG", "600": "PRY", "604": "PER", "608": "PHL",
    "616": "POL", "620": "PRT", "634": "QAT", "642": "ROU", "643": "RUS",
    "646": "RWA", "659": "KNA", "662": "LCA", "670": "VCT", "882": "WSM",
    "674": "SMR", "678": "STP", "682": "SAU", "686": "SEN", "688": "SRB",
    "690": "SYC", "694": "SLE", "702": "SGP", "703": "SVK", "705": "SVN",
    "90": "SLB", "706": "SOM", "710": "ZAF", "724": "ESP", "144": "LKA",
    "736": "SDN", "740": "SUR", "748": "SWZ", "752": "SWE", "756": "CHE",
    "760": "SYR", "158": "TWN", "762": "TJK", "834": "TZA", "764": "THA",
    "626": "TLS", "768": "TGO", "776": "TON", "780": "TTO", "788": "TUN",
    "792": "TUR", "795": "TKM", "798": "TUV", "800": "UGA", "804": "UKR",
    "784": "ARE", "826": "GBR", "840": "USA", "858": "URY", "860": "UZB",
    "548": "VUT", "862": "VEN", "704": "VNM", "887": "YEM", "894": "ZMB",
    "716": "ZWE",
    # Territories / special areas
    "275": "PSE", "531": "CUW", "534": "SXM", "535": "BES",
    "630": "PRI", "316": "GUM", "580": "MNP", "850": "VIR",
    "796": "TCA", "136": "CYM", "060": "BMU", "092": "VGB",
}

# M49 numeric → country name mapping
M49_TO_NAME = {
    "4": "Afghanistan", "8": "Albania", "12": "Algeria", "32": "Argentina",
    "36": "Australia", "40": "Austria", "31": "Azerbaijan", "48": "Bahrain",
    "50": "Bangladesh", "56": "Belgium", "68": "Bolivia", "76": "Brazil",
    "100": "Bulgaria", "116": "Cambodia", "120": "Cameroon", "124": "Canada",
    "152": "Chile", "156": "China", "170": "Colombia", "188": "Costa Rica",
    "191": "Croatia", "196": "Cyprus", "203": "Czech Republic", "208": "Denmark",
    "214": "Dominican Republic", "218": "Ecuador", "818": "Egypt",
    "233": "Estonia", "231": "Ethiopia", "246": "Finland", "250": "France",
    "268": "Georgia", "276": "Germany", "288": "Ghana", "300": "Greece",
    "320": "Guatemala", "332": "Haiti", "340": "Honduras", "344": "Hong Kong",
    "348": "Hungary", "352": "Iceland", "356": "India", "360": "Indonesia",
    "364": "Iran", "368": "Iraq", "372": "Ireland", "376": "Israel",
    "380": "Italy", "388": "Jamaica", "392": "Japan", "400": "Jordan",
    "398": "Kazakhstan", "404": "Kenya", "410": "South Korea", "414": "Kuwait",
    "417": "Kyrgyzstan", "428": "Latvia", "422": "Lebanon", "440": "Lithuania",
    "442": "Luxembourg", "446": "Macao", "458": "Malaysia", "484": "Mexico",
    "496": "Mongolia", "499": "Montenegro", "504": "Morocco", "508": "Mozambique",
    "104": "Myanmar", "524": "Nepal", "528": "Netherlands", "554": "New Zealand",
    "558": "Nicaragua", "566": "Nigeria", "578": "Norway", "512": "Oman",
    "586": "Pakistan", "591": "Panama", "600": "Paraguay", "604": "Peru",
    "608": "Philippines", "616": "Poland", "620": "Portugal", "634": "Qatar",
    "642": "Romania", "643": "Russia", "682": "Saudi Arabia", "686": "Senegal",
    "688": "Serbia", "702": "Singapore", "703": "Slovakia", "705": "Slovenia",
    "710": "South Africa", "724": "Spain", "144": "Sri Lanka", "736": "Sudan",
    "752": "Sweden", "756": "Switzerland", "764": "Thailand", "780": "Trinidad and Tobago",
    "788": "Tunisia", "792": "Turkey", "804": "Ukraine", "784": "United Arab Emirates",
    "826": "United Kingdom", "840": "United States", "858": "Uruguay",
    "860": "Uzbekistan", "862": "Venezuela", "704": "Vietnam",
}

# ISO2 → ISO3 mapping (same as in BIS scraper, centralized here for transform)
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
    "ZW": "ZWE",
}


# ISO3 → country name mapping (built from M49 dicts + manual additions)
ISO3_TO_NAME = {iso3: M49_TO_NAME[m49] for m49, iso3 in M49_TO_ISO3.items() if m49 in M49_TO_NAME}
ISO3_TO_NAME.update({
    "AND": "Andorra", "ANT": "Netherlands Antilles", "COK": "Cook Islands",
    "CUB": "Cuba", "FLK": "Falkland Islands", "GGY": "Guernsey",
    "GIB": "Gibraltar", "IMN": "Isle of Man", "JEY": "Jersey",
    "KOS": "Kosovo", "LIE": "Liechtenstein", "MHL": "Marshall Islands",
    "REU": "Réunion", "SHN": "Saint Helena", "TKL": "Tokelau",
    "TKM": "Turkmenistan", "WLF": "Wallis and Futuna",
    "ASM": "American Samoa", "ATG": "Antigua and Barbuda", "BHS": "Bahamas",
    "BRB": "Barbados", "BLZ": "Belize", "BEN": "Benin", "BTN": "Bhutan",
    "BIH": "Bosnia and Herzegovina", "BWA": "Botswana", "BRN": "Brunei",
    "BFA": "Burkina Faso", "BDI": "Burundi", "CPV": "Cape Verde",
    "TCD": "Chad", "COM": "Comoros", "COG": "Congo", "COD": "DR Congo",
    "CRI": "Costa Rica", "CIV": "Côte d'Ivoire", "DJI": "Djibouti",
    "DMA": "Dominica", "SLV": "El Salvador", "GNQ": "Equatorial Guinea",
    "ERI": "Eritrea", "FJI": "Fiji", "GAB": "Gabon", "GMB": "Gambia",
    "GRD": "Grenada", "GIN": "Guinea", "GNB": "Guinea-Bissau",
    "GUY": "Guyana", "KIR": "Kiribati", "PRK": "North Korea",
    "LAO": "Laos", "LSO": "Lesotho", "LBR": "Liberia", "LBY": "Libya",
    "MAC": "Macao", "MKD": "North Macedonia", "MDG": "Madagascar",
    "MWI": "Malawi", "MDV": "Maldives", "MLI": "Mali", "MLT": "Malta",
    "MRT": "Mauritania", "MUS": "Mauritius", "FSM": "Micronesia",
    "MDA": "Moldova", "MCO": "Monaco", "NAM": "Namibia", "NRU": "Nauru",
    "NER": "Niger", "PLW": "Palau", "PNG": "Papua New Guinea",
    "PRY": "Paraguay", "RWA": "Rwanda", "KNA": "Saint Kitts and Nevis",
    "LCA": "Saint Lucia", "VCT": "Saint Vincent", "WSM": "Samoa",
    "SMR": "San Marino", "STP": "São Tomé and Príncipe",
    "SYC": "Seychelles", "SLE": "Sierra Leone", "SLB": "Solomon Islands",
    "SOM": "Somalia", "SUR": "Suriname", "SWZ": "Eswatini",
    "SYR": "Syria", "TWN": "Taiwan", "TZA": "Tanzania", "TLS": "Timor-Leste",
    "TGO": "Togo", "TON": "Tonga", "TTO": "Trinidad and Tobago",
    "TUV": "Tuvalu", "UGA": "Uganda", "VUT": "Vanuatu", "YEM": "Yemen",
    "PSE": "Palestine", "CUW": "Curaçao", "SXM": "Sint Maarten",
    "BES": "Bonaire", "PRI": "Puerto Rico", "GUM": "Guam",
    "MNP": "Northern Mariana Islands", "VIR": "US Virgin Islands",
    "TCA": "Turks and Caicos", "CYM": "Cayman Islands", "BMU": "Bermuda",
    "VGB": "British Virgin Islands",
})


def m49_to_iso3(m49_code: str) -> str:
    """Convert M49 numeric code to ISO3 alpha-3 code. Returns None if not found."""
    clean = str(m49_code).strip().strip("'\"")
    return M49_TO_ISO3.get(clean)


def m49_to_name(m49_code: str) -> str:
    """Convert M49 numeric code to country name. Returns the code if not found."""
    clean = str(m49_code).strip().strip("'\"")
    return M49_TO_NAME.get(clean, clean)


def iso2_to_iso3(iso2_code: str) -> str:
    """Convert ISO2 alpha-2 code to ISO3 alpha-3 code. Returns None if not found."""
    clean = str(iso2_code).strip().upper()
    return ISO2_TO_ISO3.get(clean)
