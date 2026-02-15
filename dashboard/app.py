import os

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = "diploma"
COLLECTION = "economic_indicators"

INDICATOR_LABELS = {
    "cpi": "CPI",
    "food_cpi": "Food CPI",
    "housing_price_index": "Housing Price Index",
    "monthly_wage": "Monthly Wage",
}

INDICATOR_COLORS = {
    "CPI": "#636EFA",
    "Food CPI": "#EF553B",
    "Housing Price Index": "#00CC96",
    "Monthly Wage": "#AB63FA",
}


@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_URI)
    return client[MONGO_DB][COLLECTION]


@st.cache_data(ttl=300)
def load_all_data():
    col = get_mongo_collection()
    cursor = col.find(
        {"value": {"$ne": None}},
        {"_id": 0, "country_code": 1, "country_name": 1, "year": 1,
         "value": 1, "value_usd": 1, "indicator_type": 1},
    )
    df = pd.DataFrame(list(cursor))
    if df.empty:
        return df
    df["indicator_label"] = df["indicator_type"].map(INDICATOR_LABELS)
    return df


def rebase_dynamic(df, group_cols, value_col="value"):
    """Rebase each series to its earliest year in the visible range = 100."""
    base = df.loc[df.groupby(group_cols)["year"].idxmin(), group_cols + [value_col]]
    base = base.rename(columns={value_col: "_base"})
    merged = df.merge(base, on=group_cols, how="left")
    merged["index_value"] = (merged[value_col] / merged["_base"] * 100).round(2)
    merged = merged.drop(columns=["_base"])
    return merged


@st.cache_data(ttl=300)
def get_stats():
    col = get_mongo_collection()
    total = col.count_documents({})
    countries = len(col.distinct("country_code"))
    years = list(col.aggregate([
        {"$group": {"_id": None, "min_y": {"$min": "$year"}, "max_y": {"$max": "$year"}}}
    ]))
    year_range = (years[0]["min_y"], years[0]["max_y"]) if years else (0, 0)

    per_indicator = list(col.aggregate([
        {"$group": {
            "_id": "$indicator_type",
            "records": {"$sum": 1},
            "countries": {"$addToSet": "$country_code"},
        }},
        {"$project": {
            "indicator": "$_id",
            "records": 1,
            "country_count": {"$size": "$countries"},
        }},
        {"$sort": {"indicator": 1}},
    ]))

    return total, countries, year_range, per_indicator


# --- Page config ---
st.set_page_config(page_title="Economic Indicators", layout="wide")
st.title("Economic Indicators Dashboard")

# --- Sidebar: data overview ---
total, countries, year_range, per_indicator = get_stats()

st.sidebar.header("Data Overview")
st.sidebar.metric("Total Records", f"{total:,}")
st.sidebar.metric("Countries", countries)
st.sidebar.metric("Year Range", f"{year_range[0]}–{year_range[1]}")

st.sidebar.subheader("Per Indicator")
for item in per_indicator:
    label = INDICATOR_LABELS.get(item["indicator"], item["indicator"])
    st.sidebar.text(f"{label}: {item['records']:,} records, {item['country_count']} countries")

# --- Load data ---
df = load_all_data()

if df.empty:
    st.warning("No data found in MongoDB. Run the ETL pipeline first.")
    st.stop()

country_names = df.drop_duplicates("country_code").set_index("country_code")["country_name"]

# --- Tabs ---
tab_country, tab_compare = st.tabs(["Country View", "Cross-Country Comparison"])


# --- Tab 1: Country View ---
@st.fragment
def render_country_view():
    country_options = sorted(df["country_code"].unique())

    display_options = [f"{c} — {country_names.get(c, '')}" for c in country_options]
    selected_display = st.selectbox("Select country", display_options, key="country_select")
    selected_country = selected_display.split(" — ")[0]

    country_df = df[df["country_code"] == selected_country].copy()

    min_year = int(country_df["year"].min())
    max_year = int(country_df["year"].max())
    year_slider = st.slider("Year range", min_year, max_year, (min_year, max_year))
    country_df = country_df[
        (country_df["year"] >= year_slider[0]) & (country_df["year"] <= year_slider[1])
    ]

    # USD-equivalent toggle
    has_usd = "value_usd" in country_df.columns and country_df["value_usd"].notna().any()
    use_usd = st.checkbox("USD equivalent", value=False, disabled=not has_usd)

    if use_usd and has_usd:
        country_df = country_df[country_df["value_usd"].notna()]
        val_col = "value_usd"
    else:
        val_col = "value"

    # Common base year = latest first year among visible indicators
    base_year = int(country_df.groupby("indicator_type")["year"].min().max())

    # Rebase all indicators to the common base year = 100
    base_values = country_df[country_df["year"] == base_year][["indicator_type", val_col]].copy()
    base_values = base_values.rename(columns={val_col: "_base"})
    country_df = country_df.merge(base_values, on="indicator_type", how="left")
    country_df["index_value"] = (country_df[val_col] / country_df["_base"] * 100).round(2)
    country_df = country_df.drop(columns=["_base"])

    subtitle = " (USD equivalent)" if use_usd and has_usd else ""
    fig = px.line(
        country_df.sort_values("year"),
        x="year",
        y="index_value",
        color="indicator_label",
        color_discrete_map=INDICATOR_COLORS,
        markers=True,
        labels={"year": "Year", "index_value": f"Index ({base_year} = 100)",
                "indicator_label": "Indicator"},
        title=f"{selected_country} — {country_names.get(selected_country, '')}{subtitle}",
    )
    fig.add_hline(y=100, line_dash="dash", line_color="gray",
                  annotation_text=f"{base_year} baseline")
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    if not has_usd:
        st.caption("USD equivalent unavailable — exchange rate data missing for some indicators.")


with tab_country:
    render_country_view()


# --- Tab 2: Cross-Country Comparison ---
@st.fragment
def render_cross_country():
    indicator_options = sorted(df["indicator_type"].unique())
    indicator_display = [INDICATOR_LABELS.get(i, i) for i in indicator_options]
    selected_indicator_display = st.selectbox("Select indicator", indicator_display)
    selected_indicator = indicator_options[indicator_display.index(selected_indicator_display)]

    all_countries = sorted(df["country_code"].unique())
    all_display = [f"{c} — {country_names.get(c, '')}" for c in all_countries]

    selected_compare = st.multiselect(
        "Select countries", all_display, max_selections=8,
        key="_compare_sel",
    )
    selected_codes = [s.split(" — ")[0] for s in selected_compare]

    if len(selected_codes) >= 1:
        compare_df = df[
            (df["country_code"].isin(selected_codes))
            & (df["indicator_type"] == selected_indicator)
        ].copy()

        missing = set(selected_codes) - set(compare_df["country_code"].unique())
        if missing:
            missing_names = [f"{c} — {country_names.get(c, '')}" for c in sorted(missing)]
            st.caption(f"No {selected_indicator_display} data for: {', '.join(missing_names)}")

        if compare_df.empty:
            st.info("No data for the selected indicator and countries.")
        else:
            comp_min_year = int(compare_df["year"].min())
            comp_max_year = int(compare_df["year"].max())
            comp_year_slider = st.slider(
                "Year range", comp_min_year, comp_max_year,
                (comp_min_year, comp_max_year), key="compare_year_range",
            )
            compare_df = compare_df[
                (compare_df["year"] >= comp_year_slider[0])
                & (compare_df["year"] <= comp_year_slider[1])
            ]

            # Base year = earliest year where ALL selected countries have data
            years_with_all = (
                compare_df.groupby("year")["country_code"]
                .nunique()
                .loc[lambda s: s == compare_df["country_code"].nunique()]
            )
            common_base_year = int(years_with_all.index.min())

            # USD-adjusted toggle
            has_usd = "value_usd" in compare_df.columns and compare_df["value_usd"].notna().any()
            comp_use_usd = st.checkbox("USD adjusted", value=False, disabled=not has_usd,
                                       key="compare_usd")

            if comp_use_usd and has_usd:
                compare_df = compare_df[compare_df["value_usd"].notna()]
                val_col = "value_usd"
                subtitle = " (USD-adjusted)"
            else:
                val_col = "value"
                subtitle = ""

            # Rebase all countries to the common base year = 100
            base_values = (
                compare_df[compare_df["year"] == common_base_year][["country_code", val_col]]
                .rename(columns={val_col: "_base"})
            )
            compare_df = compare_df.merge(base_values, on="country_code", how="left")
            compare_df["index_value"] = (compare_df[val_col] / compare_df["_base"] * 100).round(2)
            compare_df = compare_df.drop(columns=["_base"])

            compare_df["country_label"] = compare_df.apply(
                lambda r: f"{r['country_code']} — {r['country_name']}", axis=1
            )

            fig2 = px.line(
                compare_df.sort_values("year"),
                x="year",
                y="index_value",
                color="country_label",
                markers=True,
                labels={"year": "Year", "index_value": f"Index ({common_base_year} = 100)",
                        "country_label": "Country"},
                title=f"{selected_indicator_display} — Cross-Country Comparison{subtitle}",
            )
            fig2.add_hline(y=100, line_dash="dash", line_color="gray",
                           annotation_text=f"{common_base_year} baseline")
            fig2.update_layout(height=500)
            st.plotly_chart(fig2, use_container_width=True)

            if not has_usd:
                st.caption("USD adjustment unavailable — exchange rate data missing for some countries.")


with tab_compare:
    render_cross_country()
