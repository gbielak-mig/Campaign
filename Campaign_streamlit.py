import time
import streamlit as st
import pandas as pd
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, FilterExpression,
    Filter, FilterExpressionList,
)
from google.cloud import bigquery
from datetime import timedelta, date

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
st.set_page_config(page_title="GA4 + Social Dashboard", layout="wide", page_icon="📊")

GA4_DIMENSIONS = ["sessionSource", "sessionMedium", "sessionCampaignName"]
GA4_METRICS    = ["sessions", "transactions", "totalRevenue", "advertiserAdCost"]

PLATFORM_FILTERS = {
    "Web": ["WEB"],
    "App": ["IOS", "ANDROID"],
}

ADS_SOURCES    = {"google", "google ads", "googleads", "bing", "microsoft"}
META_SOURCES   = {"facebook", "instagram", "fb", "meta"}
TIKTOK_SOURCES = {"tiktok", "tik tok", "tiktok.com"}

DATE_PRESETS = {
    "Ostatnie 7 dni":  7,
    "Ostatnie 14 dni": 14,
    "Ostatnie 30 dni": 30,
    "Ostatnie 60 dni": 60,
    "Ostatnie 90 dni": 90,
    "Własny zakres":   None,
}

META_ALIAS_TO_MPK = {
    "50PL":"S501","BS":"S514","SPL":"S500","SDE":"G500",
    "SCZ":"CZ50","SSK":"SK50","SLT":"LT50","SRO":"RO50",
    "SYM":"S502","TBL":"S507","JDPL":"S512","JDRO":"RO55",
    "JDSK":"SK52","JDHU":"HU52","JDLT":"LT52","JDBG":"BG52",
    "JDCZ":"CZ55","JDUA":"UA52","JDHR":"HR52",
}

def extract_meta_alias(name: str) -> str:
    return name.split("-")[0].strip().upper() if name else ""

# ─────────────────────────────────────────────
# MAPPINGS
# ─────────────────────────────────────────────
def build_property_mapping():
    rows = []
    for mpk, vals in st.secrets["ga4_properties"].items():
        rows.append({
            "MPK":      mpk,
            "ID_GA4":   int(vals[0]),
            "Brand":    vals[1],
            "Currency": vals[2] if len(vals) > 2 else "",
            "TT_Alias": vals[3] if len(vals) > 3 else "",
        })
    return pd.DataFrame(rows)

property_mapping = build_property_mapping()

tt_alias_to_mpk = {
    r["TT_Alias"]: r["MPK"]
    for _, r in property_mapping.iterrows() if r["TT_Alias"]
}

# ─────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────
if not st.session_state.get("authenticated"):
    st.title("🔐 GA4 + Social Dashboard")
    pwd = st.text_input("Hasło:", type="password")
    if st.button("Zaloguj", use_container_width=True):
        if pwd == st.secrets["app"]["password"]:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("❌ Błędne hasło!")
    st.stop()

# ─────────────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────────────
@st.cache_resource
def get_ga4_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)

def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/bigquery.readonly",
            "https://www.googleapis.com/auth/cloud-platform",
        ],
    )
    return bigquery.Client(credentials=creds)

try:
    ga4_client = get_ga4_client()
    bq_client  = get_bq_client()
except Exception as e:
    st.error(f"Błąd połączenia: {e}")
    st.stop()

yesterday = date.today() - timedelta(days=1)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
def build_platform_filter(vals):
    if not vals:
        return None
    exprs = [FilterExpression(filter=Filter(
        field_name="platform",
        string_filter=Filter.StringFilter(
            match_type=Filter.StringFilter.MatchType.EXACT,
            value=v, case_sensitive=False,
        )
    )) for v in vals]
    return exprs[0] if len(exprs) == 1 else FilterExpression(
        or_group=FilterExpressionList(expressions=exprs)
    )

# ─────────────────────────────────────────────
# DATA FETCHING – GA4
# ─────────────────────────────────────────────
def get_ga4_data(property_id, start_date, end_date, platform_expr=None):
    try:
        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in GA4_DIMENSIONS],
            metrics=[Metric(name=m) for m in GA4_METRICS],
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            dimension_filter=platform_expr,
        )
        resp = ga4_client.run_report(req)
        rows = []
        for row in resp.rows:
            rd = {GA4_DIMENSIONS[i]: v.value for i, v in enumerate(row.dimension_values)}
            for i, mv in enumerate(row.metric_values):
                rd[GA4_METRICS[i]] = mv.value
            rows.append(rd)
        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=GA4_DIMENSIONS + GA4_METRICS)
        for m in GA4_METRICS:
            if m in df.columns:
                df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0)
        return df
    except Exception as e:
        st.warning(f"GA4 błąd {property_id}: {e}")
        return pd.DataFrame(columns=GA4_DIMENSIONS + GA4_METRICS)

# ─────────────────────────────────────────────
# DATA FETCHING – Facebook / Meta (BQ)
# ─────────────────────────────────────────────
def get_facebook_data(start_date, end_date):
    q = f"""
        SELECT CampaignName, CampaignId, Clicks, Spend
        FROM `facebook-423312.meta.AdInsights`
        WHERE DateStart BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        df = bq_client.query(q, location="europe-west3").to_dataframe()
    except Exception as e:
        st.warning(f"Facebook BQ błąd: {e}")
        return pd.DataFrame()
    if df.empty:
        return df
    df["_alias"]        = df["CampaignName"].astype(str).apply(extract_meta_alias)
    df["MPK"]           = df["_alias"].map(META_ALIAS_TO_MPK).fillna("")
    df["Spend"]         = pd.to_numeric(df["Spend"],  errors="coerce").fillna(0)
    df["Clicks"]        = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
    df["CampaignName"]  = df["CampaignName"].astype(str)
    return df.drop(columns=["_alias"])

# ─────────────────────────────────────────────
# DATA FETCHING – TikTok (BQ)
# ─────────────────────────────────────────────
def get_tiktok_data(start_date, end_date):
    q = f"""
        SELECT advertiser_name, campaign_id, campaign_name, spend
        FROM `facebook-423312.tiktok.tik_tok`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        df = bq_client.query(q, location="europe-west3").to_dataframe()
    except Exception as e:
        st.warning(f"TikTok BQ błąd: {e}")
        return pd.DataFrame()
    if df.empty:
        return df
    df["MPK"] = (
        df["advertiser_name"].astype(str).str.strip()
        .map(tt_alias_to_mpk)
        .fillna(df["advertiser_name"].astype(str).str.strip().map(META_ALIAS_TO_MPK))
        .fillna("")
    )
    df["spend"]        = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
    df["campaign_name"] = df["campaign_name"].astype(str)
    return df

# ─────────────────────────────────────────────
# BUILD TABLES (3 osobne)
# ─────────────────────────────────────────────
def build_ga4_table(combined_ga4: pd.DataFrame, selected_mpk_set: set) -> pd.DataFrame:
    """
    Tabela 1: Przychody + koszty z GA4.
    Kolumny: MPK, Brand, Źródło, CampaignName, Sessions, Transactions, Revenue, AdCost
    """
    if combined_ga4.empty:
        return pd.DataFrame(columns=["MPK","Brand","Źródło","sessionMedium",
                                      "CampaignName","Sessions","Transactions",
                                      "Revenue","AdCost"])

    df = combined_ga4.copy()
    df["Źródło"] = df["sessionSource"].str.lower().str.strip().apply(
        lambda s: "ADS" if s in ADS_SOURCES
        else ("Meta" if s in META_SOURCES
              else ("TikTok" if s in TIKTOK_SOURCES else "Other"))
    )

    result = (
        df.groupby(["MPK","Brand","Źródło","sessionMedium","sessionCampaignName"], as_index=False)
        .agg(
            Sessions     =("sessions",         "sum"),
            Transactions =("transactions",      "sum"),
            Revenue      =("totalRevenue",      "sum"),
            AdCost       =("advertiserAdCost",  "sum"),
        )
        .rename(columns={"sessionCampaignName": "CampaignName"})
    )

    if selected_mpk_set:
        result = result[result["MPK"].isin(selected_mpk_set)]

    return result.sort_values(["Źródło","MPK","Revenue"], ascending=[True,True,False]).reset_index(drop=True)


def build_meta_bq_table(fb_df: pd.DataFrame, selected_mpk_set: set) -> pd.DataFrame:
    """
    Tabela 2: Meta Spend z BigQuery.
    Kolumny: MPK, CampaignId, CampaignName, Clicks, Spend
    """
    if fb_df is None or fb_df.empty:
        return pd.DataFrame(columns=["MPK","CampaignId","CampaignName","Clicks","Spend"])

    result = (
        fb_df.groupby(["MPK","CampaignId","CampaignName"], as_index=False)
        .agg(Clicks=("Clicks","sum"), Spend=("Spend","sum"))
    )
    result["CampaignId"] = result["CampaignId"].astype(str)

    if selected_mpk_set:
        result = result[result["MPK"].isin(selected_mpk_set)]

    return result.sort_values(["MPK","Spend"], ascending=[True,False]).reset_index(drop=True)


def build_tiktok_bq_table(tt_df: pd.DataFrame, selected_mpk_set: set) -> pd.DataFrame:
    """
    Tabela 3: TikTok Spend z BigQuery.
    Kolumny: MPK, campaign_id, campaign_name, spend
    """
    if tt_df is None or tt_df.empty:
        return pd.DataFrame(columns=["MPK","campaign_id","campaign_name","spend"])

    result = (
        tt_df.groupby(["MPK","campaign_id","campaign_name"], as_index=False)
        .agg(spend=("spend","sum"))
    )
    result["campaign_id"] = result["campaign_id"].astype(str)

    if selected_mpk_set:
        result = result[result["MPK"].isin(selected_mpk_set)]

    return result.sort_values(["MPK","spend"], ascending=[True,False]).reset_index(drop=True)

# ─────────────────────────────────────────────
# FETCH ALL (cached)
# ─────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def fetch_all(mpk_tuple, start_date, end_date, platform_choice, id_map_json):
    from io import StringIO
    id_map = pd.read_json(StringIO(id_map_json))

    if platform_choice == "Web + App":
        pexpr = None
    elif platform_choice == "Web":
        pexpr = build_platform_filter(PLATFORM_FILTERS["Web"])
    else:
        pexpr = build_platform_filter(PLATFORM_FILTERS["App"])

    combined_ga4 = pd.DataFrame()
    for _, row in id_map.iterrows():
        df = get_ga4_data(row["ID_GA4"], start_date, end_date, pexpr)
        if df is not None and not df.empty:
            df["MPK"]   = row["MPK"]
            df["Brand"] = row["Brand"]
            combined_ga4 = pd.concat([combined_ga4, df], ignore_index=True)

    fb_df = get_facebook_data(start_date, end_date)
    if not fb_df.empty and mpk_tuple:
        fb_df = fb_df[fb_df["MPK"].isin(mpk_tuple)]

    tt_df = get_tiktok_data(start_date, end_date)
    if not tt_df.empty and mpk_tuple:
        tt_df = tt_df[tt_df["MPK"].isin(mpk_tuple)]

    return combined_ga4, fb_df, tt_df

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.title("📊 Dashboard")
    st.markdown("---")

    st.subheader("📱 Platforma GA4")
    platform_choice = st.radio("Platforma:", ["Web", "App", "Web + App"], index=2)
    st.markdown("---")

    st.subheader("🏬 Sklep")
    brand_options   = sorted(property_mapping["Brand"].unique())
    selected_brands = st.multiselect("Brand:", options=brand_options, default=[])
    filtered_map    = property_mapping[property_mapping["Brand"].isin(selected_brands)] if selected_brands else property_mapping.copy()

    mpk_options   = sorted(filtered_map["MPK"].tolist())
    selected_mpks = st.multiselect("MPK:", options=mpk_options, default=[])
    if selected_mpks:
        filtered_map = filtered_map[filtered_map["MPK"].isin(selected_mpks)]

    st.caption(f"Wybrano {len(filtered_map)} sklep(ów)")
    selected_mpk_set = set(filtered_map["MPK"].tolist())
    st.markdown("---")

    st.subheader("📅 Zakres dat")
    preset_label = st.selectbox("Preset:", list(DATE_PRESETS.keys()), index=0)
    preset_days  = DATE_PRESETS[preset_label]

    if preset_days is not None:
        start_date = yesterday - timedelta(days=preset_days - 1)
        end_date   = yesterday
        st.caption(f"📆 {start_date} → {end_date}")
    else:
        start_date = st.date_input("Od:", value=yesterday - timedelta(days=6), max_value=yesterday)
        end_date   = st.date_input("Do:", value=yesterday, max_value=yesterday)
        if start_date > end_date:
            st.error("Data 'Od' musi być wcześniejsza!")

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.title("📊 GA4 + Social Dashboard")
h1, h2, h3 = st.columns(3)
h1.metric("Sklepów",   len(filtered_map))
h2.metric("Platforma", platform_choice)
h3.metric("Zakres",    f"{start_date} → {end_date}")

# ─────────────────────────────────────────────
# AUTO-REFRESH (2s debounce)
# ─────────────────────────────────────────────
params_key = (
    tuple(sorted(selected_mpk_set)),
    str(start_date),
    str(end_date),
    platform_choice,
)
if st.session_state.get("last_params") != params_key:
    st.session_state["last_params"] = params_key
    st.session_state["pending_at"]  = time.time()

secs_left = 2 - (time.time() - st.session_state.get("pending_at", 0))
if secs_left > 0:
    time.sleep(1)
    st.rerun()

# ─────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────
if filtered_map.empty:
    st.warning("Nie wybrano sklepów.")
    st.stop()

with st.spinner("⏳ Pobieranie danych…"):
    id_map_json = filtered_map[["MPK","Brand","ID_GA4"]].to_json()
    combined_ga4, fb_df, tt_df = fetch_all(
        mpk_tuple       = tuple(sorted(selected_mpk_set)),
        start_date      = start_date,
        end_date        = end_date,
        platform_choice = platform_choice,
        id_map_json     = id_map_json,
    )

ga4_table    = build_ga4_table(combined_ga4, selected_mpk_set)
meta_table   = build_meta_bq_table(fb_df, selected_mpk_set)
tiktok_table = build_tiktok_bq_table(tt_df, selected_mpk_set)

# ─────────────────────────────────────────────
# RENDER – 3 TABELE
# ─────────────────────────────────────────────
NUM_CFG_GA4 = {
    "Revenue": st.column_config.NumberColumn(format="%.2f"),
    "AdCost":  st.column_config.NumberColumn(format="%.2f"),
}
NUM_CFG_BQ = {
    "Spend": st.column_config.NumberColumn(format="%.2f"),
    "spend": st.column_config.NumberColumn(format="%.2f"),
}

# ── Tabela 1: GA4 ─────────────────────────────
st.subheader("📊 GA4 — przychody i koszty kampanii")
if ga4_table.empty:
    st.info("Brak danych GA4.")
else:
    st.dataframe(ga4_table, use_container_width=True, height=400, column_config=NUM_CFG_GA4)
    st.caption(
        f"Wierszy: {len(ga4_table):,}  |  "
        f"Revenue: {ga4_table['Revenue'].sum():,.2f}  |  "
        f"AdCost: {ga4_table['AdCost'].sum():,.2f}"
    )

st.divider()

# ── Tabela 2: Meta BQ ─────────────────────────
st.subheader("📘 Meta — spend z BigQuery")
if meta_table.empty:
    st.info("Brak danych Meta.")
else:
    st.dataframe(meta_table, use_container_width=True, height=400, column_config=NUM_CFG_BQ)
    st.caption(
        f"Wierszy: {len(meta_table):,}  |  "
        f"Spend: {meta_table['Spend'].sum():,.2f}  |  "
        f"Clicks: {meta_table['Clicks'].sum():,.0f}"
    )

st.divider()

# ── Tabela 3: TikTok BQ ───────────────────────
st.subheader("🎵 TikTok — spend z BigQuery")
if tiktok_table.empty:
    st.info("Brak danych TikTok.")
else:
    st.dataframe(tiktok_table, use_container_width=True, height=400, column_config=NUM_CFG_BQ)
    st.caption(
        f"Wierszy: {len(tiktok_table):,}  |  "
        f"Spend: {tiktok_table['spend'].sum():,.2f}"
    )
