"""
PySpark ETL Pipeline
Ingests data from all 5 scrapers, normalizes, deduplicates,
rebases indices to a common base year, and writes to MongoDB.
"""

import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from pyspark.sql.window import Window

# Add project root to path so scrapers can be imported
sys.path.insert(0, '/app')

from transform.country_codes import M49_TO_ISO3, M49_TO_NAME


# Unified output schema
UNIFIED_SCHEMA = StructType([
    StructField("country_code", StringType(), False),
    StructField("country_name", StringType(), True),
    StructField("year", IntegerType(), False),
    StructField("value", DoubleType(), False),
    StructField("value_rebased", DoubleType(), True),
    StructField("indicator_type", StringType(), False),
    StructField("source", StringType(), False),
    StructField("ingested_at", TimestampType(), False),
])


def create_spark_session() -> SparkSession:
    """Create SparkSession with MongoDB connector"""
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
    mongo_db = os.environ.get("MONGO_DB", "diploma")

    spark = (
        SparkSession.builder
        .appName("diploma-etl")
        .config("spark.jars", ",".join([
            "/app/jars/mongo-spark-connector_2.12-10.4.0.jar",
            "/app/jars/mongodb-driver-sync-5.2.1.jar",
            "/app/jars/mongodb-driver-core-5.2.1.jar",
            "/app/jars/bson-5.2.1.jar",
        ]))
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


def ingest_oecd() -> list:
    """Ingest OECD wage data"""
    print("=" * 60)
    print("Ingesting OECD wage data...")
    from extract.oecd.scraper import OECDWageScraper
    scraper = OECDWageScraper()
    grouped = scraper.get_all_oecd_countries_wages()
    records = []
    for country_records in grouped.values():
        records.extend(country_records)
    print(f"  OECD: {len(records)} raw records")
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


# ---------------------------------------------------------------------------
# Normalize each source into unified schema rows
# ---------------------------------------------------------------------------

def normalize_worldbank(spark: SparkSession, raw: list) -> DataFrame:
    """Normalize WorldBank CPI data"""
    now = datetime.utcnow()
    rows = []
    for r in raw:
        if r.get('cpi_total') is not None and r.get('country_code'):
            rows.append((
                r['country_code'],
                r.get('country_name', ''),
                int(r['year']),
                float(r['cpi_total']),
                None,
                "cpi",
                "worldbank",
                now,
            ))
    return spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)


def normalize_oecd(spark: SparkSession, raw: list) -> DataFrame:
    """
    Normalize OECD wage data.
    Deduplication (UNIT_MEASURE=USD_PPP filter) is already done in the scraper.
    Extra safety: groupBy country+year → pick first value.
    """
    now = datetime.utcnow()
    rows = []
    for r in raw:
        if r.get('avg_annual_wage_usd') is not None and r.get('country_code'):
            rows.append((
                r['country_code'],
                r.get('country_name', ''),
                int(r['year']),
                float(r['avg_annual_wage_usd']),
                None,
                "avg_annual_wage",
                "oecd",
                now,
            ))

    df = spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)

    # Extra dedup: if scraper still returns >1 row per country+year, keep first
    w = Window.partitionBy("country_code", "year").orderBy("value")
    df = (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return df


def normalize_faostat(spark: SparkSession, raw: list) -> DataFrame:
    """
    Normalize FAOSTAT Food CPI data.
    - Convert M49 → ISO3
    - Monthly data → annual average (groupBy country+year)
    """
    now = datetime.utcnow()
    rows = []
    for r in raw:
        if r.get('cpi_food') is None:
            continue
        m49 = str(r.get('country_code', '')).strip().strip("'\"")
        iso3 = M49_TO_ISO3.get(m49)
        if not iso3:
            continue
        country_name = r.get('country_name', M49_TO_NAME.get(m49, ''))
        rows.append((
            iso3,
            country_name,
            int(r['year']),
            float(r['cpi_food']),
            None,
            "food_cpi",
            "faostat",
            now,
        ))

    df = spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)

    # Aggregate monthly → annual average
    df = (
        df.groupBy("country_code", "country_name", "year", "indicator_type", "source", "ingested_at")
        .agg(F.avg("value").alias("value"))
    )
    # Re-order columns to match schema
    df = df.select(
        "country_code", "country_name", "year", "value",
        F.lit(None).cast(DoubleType()).alias("value_rebased"),
        "indicator_type", "source", "ingested_at"
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
        if r.get('gross_wage') is not None and r.get('country_code'):
            rows.append((
                r['country_code'],
                r.get('country_name', ''),
                int(r['year']),
                float(r['gross_wage']),
                None,
                "hourly_wage",
                "ilostat",
                now,
            ))

    df = spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)

    # Deduplicate: avg per country+year
    df = (
        df.groupBy("country_code", "country_name", "year", "indicator_type", "source", "ingested_at")
        .agg(F.avg("value").alias("value"))
    )
    df = df.select(
        "country_code", "country_name", "year", "value",
        F.lit(None).cast(DoubleType()).alias("value_rebased"),
        "indicator_type", "source", "ingested_at"
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
        if r.get('housing_price_index') is not None and r.get('country_code'):
            rows.append((
                r['country_code'],
                r.get('country_name', ''),
                int(r['year']),
                float(r['housing_price_index']),
                None,
                "housing_price_index",
                "bis",
                now,
            ))
    return spark.createDataFrame(rows, schema=UNIFIED_SCHEMA)


# ---------------------------------------------------------------------------
# Rebase all indicators to 2015=100
# ---------------------------------------------------------------------------

def rebase_to_2015(df: DataFrame) -> DataFrame:
    """
    For each country+indicator, compute value_rebased = value / value_2015 * 100.
    If a country+indicator has no 2015 data, value_rebased stays null.
    The original value column is never modified.
    """
    base_2015 = (
        df.filter(F.col("year") == 2015)
        .select("country_code", "indicator_type", F.col("value").alias("_base"))
    )

    return (
        df.join(base_2015, on=["country_code", "indicator_type"], how="left")
        .withColumn(
            "value_rebased",
            F.when(
                (F.col("_base").isNotNull()) & (F.col("_base") != 0),
                F.round(F.col("value") / F.col("_base") * 100, 2)
            ).otherwise(None)
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
    print(f"Writing {df.count()} records to MongoDB ({mongo_db}.economic_indicators)...")

    (
        df.write
        .format("mongodb")
        .mode("overwrite")
        .option("connection.uri", mongo_uri)
        .option("database", mongo_db)
        .option("collection", "economic_indicators")
        .save()
    )
    print("Write to MongoDB complete.")

    # Recreate indexes (Spark overwrite drops the collection and its indexes)
    from pymongo import MongoClient, ASCENDING
    client = MongoClient(mongo_uri)
    col = client[mongo_db]["economic_indicators"]
    col.create_index(
        [("country_code", ASCENDING), ("year", ASCENDING), ("indicator_type", ASCENDING)],
        unique=True, name="idx_country_year_indicator_unique",
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
    oecd_raw = ingest_oecd()
    faostat_raw = ingest_faostat()
    ilostat_raw = ingest_ilostat()
    bis_raw = ingest_bis()

    # Step 2: Normalize each source
    print("=" * 60)
    print("Normalizing data...")

    wb_df = normalize_worldbank(spark, wb_raw)
    oecd_df = normalize_oecd(spark, oecd_raw)
    faostat_df = normalize_faostat(spark, faostat_raw)
    ilostat_df = normalize_ilostat(spark, ilostat_raw)
    bis_df = normalize_bis(spark, bis_raw)

    print(f"  WorldBank CPI:     {wb_df.count()} records")
    print(f"  OECD Wages:        {oecd_df.count()} records")
    print(f"  FAOSTAT Food CPI:  {faostat_df.count()} records")
    print(f"  ILOSTAT Wages:     {ilostat_df.count()} records")
    print(f"  BIS Housing Index: {bis_df.count()} records")

    # Step 3: Union all and drop non-country aggregates
    unified = (
        wb_df
        .unionByName(oecd_df)
        .unionByName(faostat_df)
        .unionByName(ilostat_df)
        .unionByName(bis_df)
    )
    unified = unified.filter(F.length("country_code") == 3)
    total = unified.count()
    print(f"\n  Total unified records: {total}")

    # Step 4: Rebase all indicators to 2015=100
    print("=" * 60)
    print("Rebasing all indicators to 2015=100...")
    unified = rebase_to_2015(unified)

    # Step 5: Write to MongoDB
    write_to_mongo(unified)

    # Summary
    print("=" * 60)
    print("ETL pipeline complete!")
    print(f"  Distinct countries:  {unified.select('country_code').distinct().count()}")
    print(f"  Distinct indicators: {unified.select('indicator_type').distinct().count()}")
    print(f"  Year range: {unified.agg(F.min('year')).collect()[0][0]} - {unified.agg(F.max('year')).collect()[0][0]}")
    print("  Indicator breakdown:")
    unified.groupBy("indicator_type").count().orderBy("indicator_type").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
