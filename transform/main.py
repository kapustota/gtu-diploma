"""
PySpark ETL Pipeline
Ingests data from 4 scrapers, normalizes, deduplicates,
rebases indices to a common base year, and writes to MongoDB.
"""

import os
import sys
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Add project root to path so scrapers can be imported
sys.path.insert(0, "/app")

from transform.country_codes import ISO3_TO_NAME, M49_TO_ISO3, M49_TO_NAME
from transform.redenominations import REDENOMINATIONS

# Unified output schema
UNIFIED_SCHEMA = StructType(
    [
        StructField("country_code", StringType(), False),
        StructField("country_name", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("value", DoubleType(), False),
        StructField("value_rebased", DoubleType(), True),
        StructField("value_usd", DoubleType(), True),
        StructField("indicator_type", StringType(), False),
        StructField("source", StringType(), False),
        StructField("ingested_at", TimestampType(), False),
    ]
)

FX_SCHEMA = StructType(
    [
        StructField("country_code", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("exchange_rate", DoubleType(), False),
    ]
)


def create_spark_session() -> SparkSession:
    """Create SparkSession with MongoDB connector"""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.environ.get("MONGO_DB", "diploma")

    spark = (
        SparkSession.builder.appName("diploma-etl")
        .config(
            "spark.jars",
            ",".join(
                [
                    "/app/jars/mongo-spark-connector_2.12-10.4.0.jar",
                    "/app/jars/mongodb-driver-sync-5.2.1.jar",
                    "/app/jars/mongodb-driver-core-5.2.1.jar",
                    "/app/jars/bson-5.2.1.jar",
                ]
            ),
        )
        .config("spark.mongodb.write.connection.uri", mongo_uri)
        .config("spark.mongodb.write.database", mongo_db)
        .config("spark.mongodb.write.collection", "economic_indicators")
        .config("spark.driver.memory", "2g")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------------------
# Ingest helpers — call each scraper, return list of dicts
# ---------------------------------------------------------------------------


def ingest_worldbank() -> list:
    """Ingest WorldBank CPI data"""
    print("=" * 60)
    print("Ingesting WorldBank CPI data...")
    from extract.wb.scraper import WorldBankCPIScraper

    scraper = WorldBankCPIScraper()
    grouped = scraper.get_all_countries_cpi()
    records = []
    for country_records in grouped.values():
        records.extend(country_records)
    print(f"  WorldBank: {len(records)} raw records")
    return records


def ingest_faostat() -> list:
    """Ingest FAOSTAT Food CPI data"""
    print("=" * 60)
    print("Ingesting FAOSTAT Food CPI data...")
    from extract.faostat.scraper import FAOSTATFoodCPIScraper

    scraper = FAOSTATFoodCPIScraper()
    grouped = scraper.get_all_countries_food_cpi()
    records = []
    for country_records in grouped.values():
        records.extend(country_records)
    print(f"  FAOSTAT: {len(records)} raw records")
    return records


def ingest_ilostat() -> list:
    """Ingest ILOSTAT wage data"""
    print("=" * 60)
    print("Ingesting ILOSTAT wage data...")
    from extract.ilostat.scraper import ILOSTATWageScraper

    scraper = ILOSTATWageScraper()
    grouped = scraper.get_all_countries_wages()
    records = []
    for country_records in grouped.values():
        records.extend(country_records)
    print(f"  ILOSTAT: {len(records)} raw records")
    return records


def ingest_bis() -> list:
    """Ingest BIS housing price index data"""
    print("=" * 60)
    print("Ingesting BIS housing price index data...")
    from extract.bis.scraper import BISHousingPriceScraper

    scraper = BISHousingPriceScraper()
    grouped = scraper.get_all_countries_prices()
    records = []
    for country_records in grouped.values():
        records.extend(country_records)
    print(f"  BIS: {len(records)} raw records")
    return records


def ingest_exchange_rates() -> list:
    """Ingest WorldBank exchange rate data (LCU per USD)"""
    print("=" * 60)
    print("Ingesting WorldBank exchange rate data...")
    from extract.wb_fx.scraper import WorldBankFXScraper

    scraper = WorldBankFXScraper()
    grouped = scraper.get_all_countries_fx()
    records = []
    for country_records in grouped.values():
        records.extend(country_records)
    print(f"  Exchange rates: {len(records)} raw records")
    return records


# ---------------------------------------------------------------------------
# Normalize each source into unified schema rows
# ---------------------------------------------------------------------------


def normalize_exchange_rates(spark: SparkSession, raw: list) -> DataFrame:
    """Normalize exchange rate data into a lookup DataFrame."""
    rows = []
    for r in raw:
        if r.get("exchange_rate") is not None and r.get("country_code"):
            rows.append(
                (
                    r["country_code"],
                    int(r["year"]),
                    float(r["exchange_rate"]),
                )
            )
    return spark.createDataFrame(rows, schema=FX_SCHEMA)


def normalize_worldbank(spark: SparkSession, raw: list) -> DataFrame:
    """Normalize WorldBank CPI data"""
    now = datetime.utcnow()
    rows = []
    for r in raw:
        if r.get("cpi_total") is not None and r.get("country_code"):
            rows.append(
                (
                    r["country_code"],
                    r.get("country_name", ""),
                    int(r["year"]),
                    float(r["cpi_total"]),
                    None,
                    None,
                    "cpi",
                    "worldbank",
                    now,
                )
            )
    return spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)


def normalize_faostat(spark: SparkSession, raw: list) -> DataFrame:
    """
    Normalize FAOSTAT Food CPI data.
    - Convert M49 → ISO3
    - Monthly data → annual average (groupBy country+year)
    """
    now = datetime.utcnow()
    rows = []
    for r in raw:
        if r.get("cpi_food") is None:
            continue
        m49 = str(r.get("country_code", "")).strip().strip("'\"")
        iso3 = M49_TO_ISO3.get(m49)
        if not iso3:
            continue
        country_name = r.get("country_name", M49_TO_NAME.get(m49, ""))
        rows.append(
            (
                iso3,
                country_name,
                int(r["year"]),
                float(r["cpi_food"]),
                None,
                None,
                "food_cpi",
                "faostat",
                now,
            )
        )

    df = spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)

    # Aggregate monthly → annual average
    df = df.groupBy(
        "country_code",
        "country_name",
        "year",
        "indicator_type",
        "source",
        "ingested_at",
    ).agg(F.avg("value").alias("value"))
    # Re-order columns to match schema
    df = df.select(
        "country_code",
        "country_name",
        "year",
        "value",
        F.lit(None).cast(DoubleType()).alias("value_rebased"),
        F.lit(None).cast(DoubleType()).alias("value_usd"),
        "indicator_type",
        "source",
        "ingested_at",
    )
    return df


def normalize_ilostat(spark: SparkSession, raw: list) -> DataFrame:
    """
    Normalize ILOSTAT wage data.
    Deduplicate: groupBy country+year → avg.
    """
    now = datetime.utcnow()
    rows = []
    for r in raw:
        if r.get("gross_wage") is not None and r.get("country_code"):
            rows.append(
                (
                    r["country_code"],
                    r.get("country_name", ""),
                    int(r["year"]),
                    float(r["gross_wage"]),
                    None,
                    None,
                    "monthly_wage",
                    "ilostat",
                    now,
                )
            )

    df = spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)

    # Deduplicate: avg per country+year
    df = df.groupBy(
        "country_code",
        "country_name",
        "year",
        "indicator_type",
        "source",
        "ingested_at",
    ).agg(F.avg("value").alias("value"))
    df = df.select(
        "country_code",
        "country_name",
        "year",
        "value",
        F.lit(None).cast(DoubleType()).alias("value_rebased"),
        F.lit(None).cast(DoubleType()).alias("value_usd"),
        "indicator_type",
        "source",
        "ingested_at",
    )
    return df


def normalize_bis(spark: SparkSession, raw: list) -> DataFrame:
    """
    Normalize BIS housing price index data.
    Quarterly→annual aggregation is already done in the scraper.
    """
    now = datetime.utcnow()
    rows = []
    for r in raw:
        if r.get("housing_price_index") is not None and r.get("country_code"):
            rows.append(
                (
                    r["country_code"],
                    r.get("country_name", ""),
                    int(r["year"]),
                    float(r["housing_price_index"]),
                    None,
                    None,
                    "housing_price_index",
                    "bis",
                    now,
                )
            )
    return spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)


# ---------------------------------------------------------------------------
# Compute USD-adjusted values
# ---------------------------------------------------------------------------

# Fixed EUR conversion rates (1 EUR = X units of old currency)
EUROZONE_RATES = {
    # Original 11 members (1999)
    "AUT": (1999, 13.7603),  # Austrian schilling
    "BEL": (1999, 40.3399),  # Belgian franc
    "DEU": (1999, 1.95583),  # Deutsche mark
    "ESP": (1999, 166.386),  # Spanish peseta
    "FIN": (1999, 5.94573),  # Finnish markka
    "FRA": (1999, 6.55957),  # French franc
    "IRL": (1999, 0.787564),  # Irish pound
    "ITA": (1999, 1936.27),  # Italian lira
    "LUX": (1999, 40.3399),  # Luxembourg franc
    "NLD": (1999, 2.20371),  # Dutch guilder
    "PRT": (1999, 200.482),  # Portuguese escudo
    # Later members
    "GRC": (2001, 340.750),  # Greek drachma
    "SVN": (2007, 239.640),  # Slovenian tolar
    "CYP": (2008, 0.585274),  # Cypriot pound
    "MLT": (2008, 0.429300),  # Maltese lira
    "SVK": (2009, 30.1260),  # Slovak koruna
    "EST": (2011, 15.6466),  # Estonian kroon
    "LVA": (2014, 0.702804),  # Latvian lats
    "LTU": (2015, 3.45280),  # Lithuanian litas
    "HRV": (2023, 7.53450),  # Croatian kuna
}


def normalize_redenomination_values(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Normalize currency-denominated indicator values for redenomination countries.
    Divides pre-switch values by the cumulative factor so all years are in the
    current (new) currency. Only applies to 'monthly_wage' — indices (CPI etc.)
    are dimensionless and unaffected by redenomination.

    This keeps values consistent with World Bank FX rates, which are already
    retroactively reported in the current currency.
    """
    redenominations_broadcast = spark.sparkContext.broadcast(dict(REDENOMINATIONS))

    @F.udf(DoubleType())
    def corrected_value(country_code, year, value, indicator_type):
        if indicator_type != "monthly_wage":
            return value
        denoms = redenominations_broadcast.value.get(country_code)
        if not denoms:
            return value
        for switch_year, factor in denoms:
            if year < switch_year:
                value = value / factor
        return round(value, 4)

    return df.withColumn(
        "value",
        corrected_value("country_code", "year", "value", "indicator_type"),
    )


def normalize_fx_eurozone(fx_records: list) -> list:
    """
    Convert pre-Euro exchange rates to EUR-equivalent.
    Before adoption: rate was in old_currency/USD.
    Convert to: EUR/USD = old_currency_per_usd / old_currency_per_eur.
    """
    result = []
    for r in fx_records:
        cc = r["country_code"]
        year = r["year"]
        rate = r["exchange_rate"]

        if cc in EUROZONE_RATES:
            adoption_year, conversion = EUROZONE_RATES[cc]
            if year < adoption_year:
                rate = rate / conversion

        result.append({**r, "exchange_rate": rate})
    return result


def compute_value_usd(df: DataFrame, fx_raw: list, spark: SparkSession) -> DataFrame:
    """
    Compute value_usd = value / exchange_rate using a broadcast dict.
    Avoids PySpark join issues by collecting FX data into a dict.
    """
    fx_dict = {(r["country_code"], r["year"]): r["exchange_rate"] for r in fx_raw}
    fx_broadcast = spark.sparkContext.broadcast(fx_dict)

    @F.udf(DoubleType())
    def usd_value(country_code, year, value):
        fx = fx_broadcast.value.get((country_code, year))
        if fx and fx != 0:
            return round(value / fx, 4)
        return None

    return df.withColumn("value_usd", usd_value("country_code", "year", "value"))


# ---------------------------------------------------------------------------
# Rebase all indicators to 2016=100
# ---------------------------------------------------------------------------


def rebase_to_2015(df: DataFrame) -> DataFrame:
    """
    For each country+indicator, compute value_rebased = value / value_2015 * 100.
    If a country+indicator has no 2015 data, value_rebased stays null.
    The original value column is never modified.
    """
    base_2015 = df.filter(F.col("year") == 2016).select(
        "country_code", "indicator_type", F.col("value").alias("_base")
    )

    return (
        df.join(base_2015, on=["country_code", "indicator_type"], how="left")
        .withColumn(
            "value_rebased",
            F.when(
                (F.col("_base").isNotNull()) & (F.col("_base") != 0),
                F.round(F.col("value") / F.col("_base") * 100, 2),
            ).otherwise(None),
        )
        .drop("_base")
    )


# ---------------------------------------------------------------------------
# Write to MongoDB
# ---------------------------------------------------------------------------


def write_to_mongo(df: DataFrame):
    """Write unified DataFrame to MongoDB using the Spark connector"""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.environ.get("MONGO_DB", "diploma")

    print("=" * 60)
    print(
        f"Writing {df.count()} records to MongoDB ({mongo_db}.economic_indicators)..."
    )

    (
        df.write.format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", "economic_indicators")
        .save()
    )
    print("Write to MongoDB complete.")

    # Recreate indexes (Spark overwrite drops the collection and its indexes)
    from pymongo import ASCENDING, MongoClient

    client = MongoClient(mongo_uri)
    col = client[mongo_db]["economic_indicators"]
    col.create_index(
        [
            ("country_code", ASCENDING),
            ("year", ASCENDING),
            ("indicator_type", ASCENDING),
        ],
        unique=True,
        name="idx_country_year_indicator_unique",
    )
    col.create_index(
        [("indicator_type", ASCENDING), ("year", ASCENDING)],
        name="idx_indicator_year",
    )
    col.create_index(
        [("country_code", ASCENDING)],
        name="idx_country",
    )
    client.close()
    print("MongoDB indexes recreated.")


# ---------------------------------------------------------------------------
# Main ETL
# ---------------------------------------------------------------------------


def main():
    print("=" * 60)
    print("Starting ETL pipeline")
    print(f"Timestamp: {datetime.utcnow().isoformat()}")
    print("=" * 60)

    spark = create_spark_session()

    # Step 1: Ingest raw data from all scrapers
    wb_raw = ingest_worldbank()
    faostat_raw = ingest_faostat()
    ilostat_raw = ingest_ilostat()
    bis_raw = ingest_bis()
    fx_raw = ingest_exchange_rates()

    # Step 2: Normalize each source
    print("=" * 60)
    print("Normalizing data...")

    wb_df = normalize_worldbank(spark, wb_raw)
    faostat_df = normalize_faostat(spark, faostat_raw)
    ilostat_df = normalize_ilostat(spark, ilostat_raw)
    bis_df = normalize_bis(spark, bis_raw)
    fx_normalized = normalize_fx_eurozone(fx_raw)

    print(f"  WorldBank CPI:     {wb_df.count()} records")
    print(f"  FAOSTAT Food CPI:  {faostat_df.count()} records")
    print(f"  ILOSTAT Wages:     {ilostat_df.count()} records")
    print(f"  BIS Housing Index: {bis_df.count()} records")
    print(f"  Exchange rates:    {len(fx_normalized)} records")

    # Step 3: Union all and drop non-country aggregates
    unified = wb_df.unionByName(faostat_df).unionByName(ilostat_df).unionByName(bis_df)
    unified = unified.filter(F.length("country_code") == 3)

    # Fill missing country names from ISO3 reference
    name_broadcast = spark.sparkContext.broadcast(ISO3_TO_NAME)

    @F.udf(StringType())
    def fill_name(country_code, country_name):
        if country_name:
            return country_name
        return name_broadcast.value.get(country_code, "")

    unified = unified.withColumn(
        "country_name", fill_name("country_code", "country_name")
    )

    total = unified.count()
    print(f"\n  Total unified records: {total}")

    # Step 4: Normalize redenomination values (wages only)
    print("=" * 60)
    print("Normalizing redenomination values...")
    unified = normalize_redenomination_values(unified, spark)

    # Step 5: Compute USD-adjusted values
    print("=" * 60)
    print("Computing USD-adjusted values...")
    unified = compute_value_usd(unified, fx_normalized, spark)
    usd_count = unified.filter(F.col("value_usd").isNotNull()).count()
    print(f"  Records with USD values: {usd_count}/{total}")

    # Step 6: Drop sparse wage series (<3 data points per country)
    print("=" * 60)
    print("Dropping sparse monthly_wage series (<3 data points)...")
    sparse_codes = [
        r.country_code
        for r in unified.filter(F.col("indicator_type") == "monthly_wage")
        .groupBy("country_code")
        .count()
        .filter(F.col("count") < 3)
        .collect()
    ]
    before = unified.count()
    unified = unified.filter(
        ~(
            (F.col("indicator_type") == "monthly_wage")
            & F.col("country_code").isin(sparse_codes)
        )
    )
    print(
        f"  Removed monthly_wage for {len(sparse_codes)} countries ({before - unified.count()} rows)"
    )

    # Step 7: Rebase all indicators to 2016=100
    print("=" * 60)
    print("Rebasing all indicators to 2016=100...")
    unified = rebase_to_2015(unified)

    # Step 8: Write to MongoDB
    write_to_mongo(unified)

    # Summary
    print("=" * 60)
    print("ETL pipeline complete!")
    print(f"  Distinct countries:  {unified.select('country_code').distinct().count()}")
    print(
        f"  Distinct indicators: {unified.select('indicator_type').distinct().count()}"
    )
    print(
        f"  Year range: {unified.agg(F.min('year')).collect()[0][0]} - {unified.agg(F.max('year')).collect()[0][0]}"
    )
    print("  Indicator breakdown:")
    unified.groupBy("indicator_type").count().orderBy("indicator_type").show(
        truncate=False
    )

    spark.stop()


if __name__ == "__main__":
    main()
