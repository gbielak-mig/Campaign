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
    "Web":  ["WEB"],
    "App":  ["IOS", "ANDROID"],
}

st.markdown("""
<style>
.sec{font-size:15px;font-weight:500;color:var(--color-text-primary);
     margin:20px 0 10px;padding-bottom:5px;
     border-bottom:0.5px solid var(--color-border-tertiary)}
</style>
""", unsafe_allow_html=True)

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

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────
yesterday = date.today() - timedelta(days=1)

DATE_PRESETS = {
    "Ostatnie 7 dni":  7,
    "Ostatnie 14 dni": 14,
    "Ostatnie 30 dni": 30,
    "Ostatnie 60 dni": 60,
    "Ostatnie 90 dni": 90,
    "Własny zakres":   None,
}

ADS_SOURCES    = {"google", "google ads", "googleads", "bing", "microsoft"}
META_SOURCES   = {"facebook", "instagram", "fb", "meta"}
TIKTOK_SOURCES = {"tiktok", "tik tok", "tiktok.com"}

def classify_source(src: str) -> str:
    s = (src or "").lower().strip()
    if s in ADS_SOURCES:    return "ADS"
    if s in META_SOURCES:   return "Meta"
    if s in TIKTOK_SOURCES: return "TikTok"
    return "Other"

# ─────────────────────────────────────────────
# DATA FETCHING – GA4
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
# DATA FETCHING – Facebook / Meta
# ─────────────────────────────────────────────
def get_facebook_data(start_date, end_date):
    # POPRAWKA: CampaignId zamiast AdCampaignId
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
    df["campaign_name"] = df["CampaignName"].astype(str)
    return df.drop(columns=["_alias"])

# ─────────────────────────────────────────────
# DATA FETCHING – TikTok
# ─────────────────────────────────────────────
def get_tiktok_data(start_date, end_date):
    q = f"""
        SELECT advertiser_name, campaign_id, campaign_name, spend
        FROM `facebook-423312.tiktok_tik_tok`
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
    df["spend"]         = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
    df["campaign_name"] = df["campaign_name"].astype(str)
    return df

# ─────────────────────────────────────────────
# BUILD RESULT TABLE
# ─────────────────────────────────────────────
def _join_ga4_bq(
    ga4_part:  pd.DataFrame,
    bq_part:   pd.DataFrame,
    brand_map: dict,
    label:     str,
) -> pd.DataFrame:
    COLS = ["MPK","Brand","Źródło","CampaignId","CampaignName","Przychód","Spend"]

    def _ensure(df):
        for c in COLS:
            if c not in df.columns:
                df[c] = "" if c in ("Brand","Źródło","CampaignId","CampaignName") else 0.0
        return df

    if ga4_part.empty and bq_part.empty:
        return pd.DataFrame(columns=COLS)

    if ga4_part.empty:
        out = _ensure(bq_part.copy())
        out["Przychód"] = 0.0
        out["Źródło"]   = label
        return out[COLS]

    if bq_part.empty:
        out = _ensure(ga4_part.copy())
        out["Spend"]      = 0.0
        out["CampaignId"] = ""
        out["Źródło"]     = label
        return out[COLS]

    ga4 = ga4_part.copy()
    bq  = bq_part.copy()
    bq["CampaignId"] = bq["CampaignId"].astype(str).str.strip()

    # Próba dopasowania po ID wyciągniętym z nazwy GA4 (10+ cyfr)
    ga4["_id"] = ga4["CampaignName"].astype(str).str.extract(r"(\d{10,})", expand=False).fillna("")
    matched_bq_ids = set()

    frames = []

    by_id = ga4[ga4["_id"] != ""].merge(
        bq.rename(columns={"CampaignName": "_bq_name"}),
        left_on=["MPK","_id"], right_on=["MPK","CampaignId"],
        how="inner",
    )
    if not by_id.empty:
        by_id["CampaignName"] = by_id["_bq_name"].fillna(by_id["CampaignName"])
        matched_bq_ids = set(by_id["CampaignId"].dropna().astype(str))
        row = by_id[["MPK","Brand","CampaignId","CampaignName","Przychód","Spend"]].copy()
        row["Brand"] = row["Brand"].fillna(row["MPK"].map(brand_map)).fillna("")
        frames.append(row)

    # Reszta — outer merge po nazwie
    ga4_rest = ga4[~ga4["_id"].isin(matched_bq_ids)].copy()
    bq_rest  = bq[~bq["CampaignId"].isin(matched_bq_ids)].copy()

    by_name = ga4_rest[["MPK","Brand","CampaignName","Przychód"]].merge(
        bq_rest[["MPK","CampaignId","CampaignName","Brand","Spend"]],
        on=["MPK","CampaignName"],
        how="outer",
        suffixes=("_ga4","_bq"),
    )
    # Scalamy Brand z obu stron
    by_name["Brand"] = by_name.get("Brand_ga4", pd.Series(dtype=str)).fillna(
        by_name.get("Brand_bq", pd.Series(dtype=str))
    ).fillna(by_name["MPK"].map(brand_map)).fillna("")
    by_name["CampaignId"] = by_name.get("CampaignId", pd.Series(dtype=str)).fillna("")
    by_name["Przychód"]   = by_name.get("Przychód",   pd.Series(dtype=float)).fillna(0.0)
    by_name["Spend"]      = by_name.get("Spend",      pd.Series(dtype=float)).fillna(0.0)
    if not by_name.empty:
        frames.append(by_name[["MPK","Brand","CampaignId","CampaignName","Przychód","Spend"]])

    if not frames:
        return pd.DataFrame(columns=COLS)

    merged = pd.concat(frames, ignore_index=True)
    for c in ["Przychód","Spend"]:
        merged[c] = pd.to_numeric(merged[c], errors="coerce").fillna(0.0)
    merged["CampaignId"] = merged["CampaignId"].fillna("").astype(str)
    merged["Źródło"]     = label
    return merged[COLS]


def build_table(
    ga4_df:          pd.DataFrame,
    fb_df:           pd.DataFrame,
    tt_df:           pd.DataFrame,
    prop_map:        pd.DataFrame,
    selected_mpk_set: set,
    source_filter:   str,
) -> pd.DataFrame:
    brand_map = prop_map.set_index("MPK")["Brand"].to_dict()
    rows = []

    # ── ADS: tylko GA4, Spend = advertiserAdCost ──
    if source_filter in ("ADS", "Wszystkie") and ga4_df is not None and not ga4_df.empty:
        ads_slice = ga4_df[ga4_df["sessionSource"].str.lower().str.strip().isin(ADS_SOURCES)].copy()
        if not ads_slice.empty:
            agg = (
                ads_slice
                .groupby(["MPK","Brand","sessionCampaignName"], as_index=False)
                .agg(Przychód=("totalRevenue","sum"), Spend=("advertiserAdCost","sum"))
                .rename(columns={"sessionCampaignName":"CampaignName"})
            )
            agg["Źródło"]     = "ADS"
            agg["CampaignId"] = ""
            rows.append(agg[["MPK","Brand","Źródło","CampaignId","CampaignName","Przychód","Spend"]])

    # ── Meta: BQ spend + GA4 revenue ──────────────
    if source_filter in ("Meta", "Wszystkie"):
        meta_ga4 = pd.DataFrame()
        if ga4_df is not None and not ga4_df.empty:
            sl = ga4_df[ga4_df["sessionSource"].str.lower().str.strip().isin(META_SOURCES)].copy()
            if not sl.empty:
                meta_ga4 = (
                    sl.groupby(["MPK","Brand","sessionCampaignName"], as_index=False)
                    .agg(Przychód=("totalRevenue","sum"))
                    .rename(columns={"sessionCampaignName":"CampaignName"})
                )

        meta_bq = pd.DataFrame()
        if fb_df is not None and not fb_df.empty:
            meta_bq = (
                fb_df.groupby(["MPK","CampaignId","campaign_name"], as_index=False)
                .agg(Spend=("Spend","sum"))
                .rename(columns={"campaign_name":"CampaignName"})
            )
            meta_bq["CampaignId"] = meta_bq["CampaignId"].astype(str)
            meta_bq["Brand"]      = meta_bq["MPK"].map(brand_map).fillna("")

        joined = _join_ga4_bq(meta_ga4, meta_bq, brand_map, "Meta")
        if not joined.empty:
            rows.append(joined)

    # ── TikTok: BQ spend + GA4 revenue ────────────
    if source_filter in ("TikTok", "Wszystkie"):
        tt_ga4 = pd.DataFrame()
        if ga4_df is not None and not ga4_df.empty:
            sl = ga4_df[ga4_df["sessionSource"].str.lower().str.strip().isin(TIKTOK_SOURCES)].copy()
            if not sl.empty:
                tt_ga4 = (
                    sl.groupby(["MPK","Brand","sessionCampaignName"], as_index=False)
                    .agg(Przychód=("totalRevenue","sum"))
                    .rename(columns={"sessionCampaignName":"CampaignName"})
                )

        tt_bq = pd.DataFrame()
        if tt_df is not None and not tt_df.empty:
            tt_bq = (
                tt_df.groupby(["MPK","campaign_id","campaign_name"], as_index=False)
                .agg(Spend=("spend","sum"))
                .rename(columns={"campaign_id":"CampaignId","campaign_name":"CampaignName"})
            )
            tt_bq["CampaignId"] = tt_bq["CampaignId"].astype(str)
            tt_bq["Brand"]      = tt_bq["MPK"].map(brand_map).fillna("")

        joined = _join_ga4_bq(tt_ga4, tt_bq, brand_map, "TikTok")
        if not joined.empty:
            rows.append(joined)

    if not rows:
        return pd.DataFrame(columns=["MPK","Brand","Źródło","CampaignId","CampaignName","Przychód","Spend"])

    result = pd.concat(rows, ignore_index=True)

    if selected_mpk_set:
        result = result[result["MPK"].isin(selected_mpk_set)]

    for c in ["Przychód","Spend"]:
        result[c] = pd.to_numeric(result[c], errors="coerce").fillna(0.0)

    return result.sort_values(["Źródło","MPK","Przychód"], ascending=[True,True,False]).reset_index(drop=True)


# ─────────────────────────────────────────────
# FETCH ALL DATA (cached by params)
# ─────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def fetch_all(mpk_tuple, start_date, end_date, platform_choice, id_map_json):
    """Pobiera dane GA4 + BQ dla podanych parametrów."""
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

    # Źródło
    st.subheader("📡 Źródło")
    source_filter = st.radio("Źródło:", ["Wszystkie", "ADS", "Meta", "TikTok"], index=0)

    st.markdown("---")

    # Platforma
    st.subheader("📱 Platforma GA4")
    platform_choice = st.radio("Platforma:", ["Web", "App", "Web + App"], index=2)

    st.markdown("---")

    # Sklep
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

    # Zakres dat
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
h1.metric("Sklepów",  len(filtered_map))
h2.metric("Platforma", platform_choice)
h3.metric("Zakres",   f"{start_date} → {end_date}")

# ─────────────────────────────────────────────
# AUTO-REFRESH z 5s opóźnieniem
# ─────────────────────────────────────────────
# Klucz do wykrywania zmiany parametrów
params_key = (
    tuple(sorted(selected_mpk_set)),
    str(start_date),
    str(end_date),
    platform_choice,
    source_filter,
)

if st.session_state.get("last_params") != params_key:
    st.session_state["last_params"]    = params_key
    st.session_state["pending_params"] = params_key
    st.session_state["pending_at"]     = time.time()

# Czekaj 5 sekund po ostatniej zmianie
pending_at = st.session_state.get("pending_at", 0)
secs_left  = 2 - (time.time() - pending_at)

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
        mpk_tuple      = tuple(sorted(selected_mpk_set)),
        start_date     = start_date,
        end_date       = end_date,
        platform_choice= platform_choice,
        id_map_json    = id_map_json,
    )

result = build_table(
    ga4_df         = combined_ga4,
    fb_df          = fb_df,
    tt_df          = tt_df,
    prop_map       = property_mapping,
    selected_mpk_set = selected_mpk_set,
    source_filter  = source_filter,
)

# ─────────────────────────────────────────────
# RENDER
# ─────────────────────────────────────────────
if result.empty:
    st.warning("Brak danych do wyświetlenia.")
    st.stop()

st.success(f"✅ Pobrano {len(result):,} wierszy")

NUM_CFG = {
    "Przychód": st.column_config.NumberColumn(format="%.2f"),
    "Spend":    st.column_config.NumberColumn(format="%.2f"),
}

st.dataframe(
    result,
    use_container_width=True,
    height=600,
    column_config=NUM_CFG,
)

st.caption(
    f"Wierszy: {len(result):,}  |  "
    f"Przychód: {result['Przychód'].sum():,.2f}  |  "
    f"Spend: {result['Spend'].sum():,.2f}"
)
