"""
Currency redenomination reference.

World Bank FX API (PA.NUS.FCRF) reports exchange rates retroactively
converted to the CURRENT local currency unit. But source data (e.g. ILO
wages) may report values in the OLD currency for years before the switch.

Each entry: ISO3 -> list of (switch_year, factor) sorted ascending.
  factor = how many old units per 1 new unit.

For a given (country, year), the FX rate must be multiplied by the
cumulative factor of all redenominations that happened AFTER that year,
so that value_old_currency / corrected_fx = correct USD amount.

Sources:
  - https://en.wikipedia.org/wiki/Redenomination
  - IMF / World Bank documentation
"""

# {ISO3: [(switch_year, old_per_new), ...]}
REDENOMINATIONS = {
    # --- Latin America ---
    "ARG": [
        (1970, 100),           # peso moneda nacional -> peso ley
        (1983, 10_000),        # peso ley -> peso argentino
        (1985, 1_000),         # peso argentino -> austral
        (1992, 10_000),        # austral -> peso convertible
    ],
    "BOL": [
        (1987, 1_000_000),     # peso boliviano -> boliviano
    ],
    "BRA": [
        (1967, 1_000),         # cruzeiro (old) -> cruzeiro novo
        (1986, 1_000),         # cruzeiro -> cruzado
        (1989, 1_000),         # cruzado -> cruzado novo
        (1993, 1_000),         # cruzeiro -> cruzeiro real
        (1994, 2_750),         # cruzeiro real -> real
    ],
    "CHL": [
        (1975, 1_000),         # escudo -> peso
    ],
    "MEX": [
        (1993, 1_000),         # old peso -> nuevo peso
    ],
    "PER": [
        (1985, 1_000),         # sol de oro -> inti
        (1991, 1_000_000),     # inti -> nuevo sol
    ],
    "URY": [
        (1975, 1_000),         # old peso -> nuevo peso
        (1993, 1_000),         # nuevo peso -> peso uruguayo
    ],
    "VEN": [
        (2008, 1_000),         # bolivar -> bolivar fuerte
        (2018, 100_000),       # bolivar fuerte -> bolivar soberano
    ],

    # --- Europe ---
    "POL": [
        (1995, 10_000),        # old zloty -> new zloty
    ],
    "ROU": [
        (2005, 10_000),        # old leu -> new leu (RON)
    ],
    "RUS": [
        (1998, 1_000),         # old ruble -> new ruble
    ],
    "TUR": [
        (2005, 1_000_000),     # old lira (TRL) -> new lira (TRY)
    ],
    "UKR": [
        (1996, 100_000),       # karbovanets -> hryvnia
    ],
    "BLR": [
        (2000, 1_000),         # old ruble -> 2nd ruble
        (2016, 10_000),        # 2nd ruble -> 3rd ruble
    ],
    "BGR": [
        (1999, 1_000),         # old lev -> new lev
    ],
    "ISL": [
        (1981, 100),           # old krona -> new krona
    ],

    # --- Africa ---
    "AGO": [
        (1995, 1_000),         # old kwanza -> kwanza reajustado
        (1999, 1_000_000),     # kwanza reajustado -> kwanza (AOA)
    ],
    "COD": [
        (1993, 3_000_000),     # old zaire -> nouveau zaire
        (1998, 100_000),       # nouveau zaire -> franc congolais
    ],
    "GHA": [
        (2007, 10_000),        # old cedi -> new cedi (GHS)
    ],
    "MOZ": [
        (2006, 1_000),         # old metical -> new metical (MZN)
    ],
    "SDN": [
        (2007, 100),           # dinar -> 3rd pound (SDG)
    ],
    "ZMB": [
        (2013, 1_000),         # old kwacha -> new kwacha (ZMW)
    ],
    "ZWE": [
        (2006, 1_000),              # 1st dollar -> 2nd dollar
        (2008, 10_000_000_000),     # 2nd dollar -> 3rd dollar
        (2009, 1_000_000_000_000),  # 3rd dollar -> 4th dollar
    ],
    "MWI": [
        # no formal redenomination, but extreme depreciation
    ],
    "STP": [
        (2018, 1_000),         # old dobra -> new dobra (STN)
    ],
    "MRT": [
        (2018, 10),            # old ouguiya -> new ouguiya (MRU)
    ],

    # --- Middle East / Central Asia ---
    "AZE": [
        (2006, 5_000),         # old manat -> new manat (AZN)
    ],
    "GEO": [
        (1995, 1_000_000),     # coupon (lari) -> lari (GEL)
    ],
    "ISR": [
        (1980, 10),            # pound -> old shekel
        (1986, 1_000),         # old shekel -> new shekel (ILS)
    ],
    "TJK": [
        (2000, 1_000),         # tajik ruble -> somoni
    ],
    "TKM": [
        (2009, 5_000),         # old manat -> new manat (TMT)
    ],
    "UZB": [
        # gradual adjustment, no single redenomination event
    ],
    "AFG": [
        (2002, 1_000),         # old afghani -> new afghani (AFN)
    ],

    # --- Asia ---
    "VNM": [
        # 1985 redenomination (÷10), but minor compared to ongoing inflation
        (1985, 10),
    ],
    "MMR": [
        # no formal redenomination since independence
    ],

    # --- Oceania / Other ---
    "SUR": [
        (2004, 1_000),         # guilder -> dollar (SRD)
    ],
}

# Remove empty entries
REDENOMINATIONS = {k: v for k, v in REDENOMINATIONS.items() if v}
