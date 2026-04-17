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
.kpi-card{background:var(--color-background-secondary);border-radius:8px;padding:14px 16px;margin-bottom:8px}
.kpi-label{font-size:12px;color:var(--color-text-secondary);margin-bottom:4px}
.kpi-value{font-size:22px;font-weight:500;color:var(--color-text-primary)}
.kpi-cmp{font-size:12px;margin-top:2px}
.pos{color:var(--color-text-success)}
.neg{color:var(--color-text-danger)}
.neu{color:var(--color-text-secondary)}
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

@st.cache_resource
def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/bigquery.readonly",
            "https://www.googleapis.com/auth/cloud-platform",
        ],
    )
    return bigquery.Client(
        credentials=creds,
        project=st.secrets["gcp_service_account"]["project_id"],
    )

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

def fmt(val, dec=0):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return f"{val:,.{dec}f}"

def kpi(label, curr, prev=None, dec=2, reverse=False):
    val_str  = fmt(curr, dec)
    cmp_html = ""
    if prev is not None and prev != 0:
        delta = curr - prev
        pct   = delta / prev * 100
        sign  = "+" if delta >= 0 else ""
        good  = delta >= 0 if not reverse else delta <= 0
        cls   = "pos" if (good and abs(pct) > 0.05) else ("neg" if abs(pct) > 0.05 else "neu")
        cmp_html = f'<div class="kpi-cmp {cls}">{sign}{fmt(delta,dec)} ({sign}{pct:.1f}%)</div>'
    return f"""<div class="kpi-card">
  <div class="kpi-label">{label}</div>
  <div class="kpi-value">{val_str}</div>
  {cmp_html}
</div>"""

def col_sum(df, col):
    return float(df[col].sum()) if (df is not None and not df.empty and col in df.columns) else 0.0

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
# DATA FETCHING – Facebook
# ─────────────────────────────────────────────
def get_facebook_data(start_date, end_date):
    q = f"""
        SELECT CampaignName, AdCampaignId, Clicks, Spend
        FROM `facebook-423312.meta.AdInsights`
        WHERE DateStart BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        df = bq_client.query(q).to_dataframe()
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
        df = bq_client.query(q).to_dataframe()
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
# CLASSIFY GA4 SOURCE → SECTION LABEL
# ─────────────────────────────────────────────
ADS_SOURCES    = {"google", "google ads", "googleads", "bing", "microsoft"}
FB_SOURCES     = {"facebook", "instagram", "fb", "meta"}
TIKTOK_SOURCES = {"tiktok", "tik tok", "tiktok.com"}

def classify_source(src: str) -> str:
    s = (src or "").lower().strip()
    if s in ADS_SOURCES:
        return "ADS"
    if s in FB_SOURCES:
        return "Facebook"
    if s in TIKTOK_SOURCES:
        return "TikTok"
    return "Other"

# ─────────────────────────────────────────────
# BUILD UNIFIED TABLE
# ─────────────────────────────────────────────
def build_unified(
    ga4_df: pd.DataFrame,
    fb_df:  pd.DataFrame,
    tt_df:  pd.DataFrame,
    prop_map: pd.DataFrame,
    selected_mpk_set: set,
) -> pd.DataFrame:
    """
    Columns: MPK, Brand, Sekcja, Source, Medium, CampaignName,
             Sesje, Transakcje, Przychód, AdCost (GA4), Spend (BQ)

    Logic:
      - ADS rows  → cost from GA4 advertiserAdCost, Spend (BQ) = NaN
      - FB rows   → join GA4 rows to Facebook BQ spend by (MPK, CampaignName)
      - TikTok    → join GA4 rows to TikTok BQ spend by (MPK, CampaignName)
      - BQ rows without GA4 match are kept (spend > 0, sessions = 0)
    """
    brand_map = prop_map.set_index("MPK")["Brand"].to_dict()

    # ── GA4 aggregate ──────────────────────────
    if ga4_df is not None and not ga4_df.empty:
        ga4_agg = (
            ga4_df
            .groupby(["MPK","Brand","sessionSource","sessionMedium","sessionCampaignName"], as_index=False)
            .agg(
                Sesje          =("sessions",         "sum"),
                Transakcje     =("transactions",      "sum"),
                Przychód       =("totalRevenue",      "sum"),
                adcost_raw     =("advertiserAdCost",  "sum"),
            )
            .rename(columns={
                "sessionSource":       "Source",
                "sessionMedium":       "Medium",
                "sessionCampaignName": "CampaignName",
            })
        )
        ga4_agg["Sekcja"] = ga4_agg["Source"].apply(classify_source)
    else:
        ga4_agg = pd.DataFrame(columns=[
            "MPK","Brand","Source","Medium","CampaignName",
            "Sesje","Transakcje","Przychód","adcost_raw","Sekcja",
        ])

    # ── BQ aggregates ─────────────────────────
    def agg_bq(df, spend_col):
        if df is None or df.empty:
            return pd.DataFrame(columns=["MPK","campaign_name","bq_spend"])
        return (
            df.groupby(["MPK","campaign_name"], as_index=False)
            .agg(bq_spend=(spend_col,"sum"))
        )

    fb_bq = agg_bq(fb_df, "Spend")
    tt_bq = agg_bq(tt_df, "spend")

    # ── Split GA4 by section ───────────────────
    ads_part  = ga4_agg[ga4_agg["Sekcja"] == "ADS"].copy()
    fb_part   = ga4_agg[ga4_agg["Sekcja"] == "Facebook"].copy()
    tt_part   = ga4_agg[ga4_agg["Sekcja"] == "TikTok"].copy()
    other_part= ga4_agg[ga4_agg["Sekcja"] == "Other"].copy()

    # ADS: cost = advertiserAdCost from GA4, no BQ spend
    ads_part["AdCost (GA4)"] = ads_part["adcost_raw"]
    ads_part["Spend (BQ)"]   = None

    # Other: no cost columns
    other_part["AdCost (GA4)"] = other_part["adcost_raw"]
    other_part["Spend (BQ)"]   = None

    def join_bq(ga4_part, bq_df, section_label):
        """Outer join GA4 rows with BQ spend rows on (MPK, CampaignName)."""
        bq_df = bq_df.rename(columns={"campaign_name": "CampaignName"})

        if ga4_part.empty and bq_df.empty:
            return pd.DataFrame()

        if ga4_part.empty:
            # BQ-only rows
            out = bq_df.copy()
            out["Brand"]       = out["MPK"].map(brand_map).fillna("")
            out["Source"]      = section_label.lower()
            out["Medium"]      = "paid social"
            out["Sekcja"]      = section_label
            out["Sesje"]       = 0.0
            out["Transakcje"]  = 0.0
            out["Przychód"]    = 0.0
            out["AdCost (GA4)"]= 0.0
            out["Spend (BQ)"]  = out["bq_spend"]
            return out[["MPK","Brand","Sekcja","Source","Medium","CampaignName",
                        "Sesje","Transakcje","Przychód","AdCost (GA4)","Spend (BQ)"]]

        merged = ga4_part.merge(bq_df, on=["MPK","CampaignName"], how="outer")
        # Fill GA4-side NaNs (BQ-only rows)
        merged["Brand"]      = merged["Brand"].fillna(merged["MPK"].map(brand_map)).fillna("")
        merged["Source"]     = merged["Source"].fillna(section_label.lower())
        merged["Medium"]     = merged["Medium"].fillna("paid social")
        merged["Sekcja"]     = section_label
        for c in ["Sesje","Transakcje","Przychód","adcost_raw"]:
            merged[c] = merged[c].fillna(0.0)
        merged["AdCost (GA4)"] = merged["adcost_raw"]
        merged["Spend (BQ)"]   = merged["bq_spend"]
        return merged[["MPK","Brand","Sekcja","Source","Medium","CampaignName",
                       "Sesje","Transakcje","Przychód","AdCost (GA4)","Spend (BQ)"]]

    fb_joined  = join_bq(fb_part,  fb_bq, "Facebook")
    tt_joined  = join_bq(tt_part,  tt_bq, "TikTok")

    # Final columns for ADS + Other
    for part in [ads_part, other_part]:
        part.drop(columns=["adcost_raw"], inplace=True, errors="ignore")

    keep_cols = ["MPK","Brand","Sekcja","Source","Medium","CampaignName",
                 "Sesje","Transakcje","Przychód","AdCost (GA4)","Spend (BQ)"]
    for part in [ads_part, other_part]:
        for c in keep_cols:
            if c not in part.columns:
                part[c] = None

    unified = pd.concat(
        [ads_part[keep_cols], fb_joined, tt_joined, other_part[keep_cols]],
        ignore_index=True,
    )

    # Filter MPK
    if selected_mpk_set:
        unified = unified[unified["MPK"].isin(selected_mpk_set)]

    # Numeric coerce
    for c in ["Sesje","Transakcje","Przychód","AdCost (GA4)","Spend (BQ)"]:
        unified[c] = pd.to_numeric(unified[c], errors="coerce")

    return (
        unified
        .sort_values(["Sekcja","MPK","Przychód"], ascending=[True,True,False])
        .reset_index(drop=True)
    )

# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.title("📊 Dashboard")
    st.markdown("---")

    st.subheader("📡 Źródła danych")
    use_ga4    = st.checkbox("GA4",      value=True)
    use_fb     = st.checkbox("Facebook", value=True)
    use_tiktok = st.checkbox("TikTok",   value=True)

    st.markdown("---")

    if use_ga4:
        st.subheader("📱 Platforma GA4")
        platform_choice = st.radio("Platforma:", ["Web","App","Web + App"], index=2)
        st.markdown("---")
    else:
        platform_choice = "Web + App"

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

    st.markdown("---")
    st.subheader("🔁 Porównanie")
    delta_days   = (end_date - start_date).days + 1
    compare_mode = st.selectbox(
        "Porównaj z:",
        ["Tydzień wstecz","Poprzedni okres","Rok wcześniej","Własny zakres","Brak"],
        index=0,
    )
    cmp_start = cmp_end = None
    if compare_mode == "Tydzień wstecz":
        cmp_start = start_date - timedelta(weeks=1)
        cmp_end   = end_date   - timedelta(weeks=1)
    elif compare_mode == "Poprzedni okres":
        cmp_end   = start_date - timedelta(days=1)
        cmp_start = cmp_end - timedelta(days=delta_days - 1)
    elif compare_mode == "Rok wcześniej":
        cmp_start = start_date - timedelta(weeks=52)
        cmp_end   = end_date   - timedelta(weeks=52)
    elif compare_mode == "Własny zakres":
        cmp_start = st.date_input("Od:", value=start_date - timedelta(days=delta_days))
        cmp_end   = st.date_input("Do:", value=start_date - timedelta(days=1))
    if cmp_start:
        st.caption(f"Cmp: {cmp_start} → {cmp_end}")

    st.markdown("---")
    run_btn = st.button("🚀 URUCHOM", use_container_width=True, type="primary")

# ─────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────
st.title("📊 GA4 + Social Dashboard")
h1, h2, h3, h4 = st.columns(4)
h1.metric("Sklepów",     len(filtered_map))
h2.metric("GA4",         platform_choice if use_ga4 else "—")
h3.metric("Zakres",      f"{start_date} → {end_date}")
h4.metric("Porównanie",  f"{cmp_start} → {cmp_end}" if cmp_start else "—")

if not run_btn:
    if "result_main" not in st.session_state:
        st.info("Ustaw parametry i kliknij **🚀 URUCHOM**.")
        st.stop()
    else:
        st.info("Dane z poprzedniego uruchomienia – kliknij 🚀 aby odświeżyć.")

# ─────────────────────────────────────────────
# FETCH
# ─────────────────────────────────────────────
if run_btn:
    if start_date > end_date:
        st.error("Błędny zakres dat!")
        st.stop()
    if filtered_map.empty:
        st.error("Nie wybrano sklepów!")
        st.stop()

    # GA4
    combined_ga4 = combined_ga4_cmp = pd.DataFrame()
    if use_ga4:
        if platform_choice == "Web + App":
            pexpr = None
        elif platform_choice == "Web":
            pexpr = build_platform_filter(PLATFORM_FILTERS["Web"])
        else:
            pexpr = build_platform_filter(PLATFORM_FILTERS["App"])

        prog = st.progress(0)
        msg  = st.empty()
        for i, (_, row) in enumerate(filtered_map.iterrows()):
            msg.write(f"⏳ GA4 – **{row['MPK']}**…")
            def enrich(df, r=row):
                if df is not None and not df.empty:
                    df = df.copy()
                    df["MPK"]   = r["MPK"]
                    df["Brand"] = r["Brand"]
                return df
            dfm = enrich(get_ga4_data(row["ID_GA4"], start_date, end_date, pexpr))
            dfc = enrich(get_ga4_data(row["ID_GA4"], cmp_start, cmp_end, pexpr)) if cmp_start else None
            if dfm is not None and not dfm.empty:
                combined_ga4 = pd.concat([combined_ga4, dfm], ignore_index=True)
            if dfc is not None and not dfc.empty:
                combined_ga4_cmp = pd.concat([combined_ga4_cmp, dfc], ignore_index=True)
            prog.progress((i + 1) / len(filtered_map))
        msg.empty(); prog.empty()

    # Facebook
    fb_df = fb_df_cmp = pd.DataFrame()
    if use_fb:
        with st.spinner("⏳ Facebook…"):
            fb_df = get_facebook_data(start_date, end_date)
            if not fb_df.empty and selected_mpk_set:
                fb_df = fb_df[fb_df["MPK"].isin(selected_mpk_set)]
            if cmp_start:
                fb_df_cmp = get_facebook_data(cmp_start, cmp_end)
                if not fb_df_cmp.empty and selected_mpk_set:
                    fb_df_cmp = fb_df_cmp[fb_df_cmp["MPK"].isin(selected_mpk_set)]

    # TikTok
    tt_df = tt_df_cmp = pd.DataFrame()
    if use_tiktok:
        with st.spinner("⏳ TikTok…"):
            tt_df = get_tiktok_data(start_date, end_date)
            if not tt_df.empty and selected_mpk_set:
                tt_df = tt_df[tt_df["MPK"].isin(selected_mpk_set)]
            if cmp_start:
                tt_df_cmp = get_tiktok_data(cmp_start, cmp_end)
                if not tt_df_cmp.empty and selected_mpk_set:
                    tt_df_cmp = tt_df_cmp[tt_df_cmp["MPK"].isin(selected_mpk_set)]

    result_main = build_unified(combined_ga4, fb_df, tt_df, property_mapping, selected_mpk_set)
    result_cmp  = (
        build_unified(combined_ga4_cmp, fb_df_cmp, tt_df_cmp, property_mapping, selected_mpk_set)
        if cmp_start else pd.DataFrame()
    )

    st.session_state["result_main"] = result_main
    st.session_state["result_cmp"]  = result_cmp
    st.session_state["cmp_active"]  = cmp_start is not None

# ─────────────────────────────────────────────
# RENDER
# ─────────────────────────────────────────────
result_main = st.session_state.get("result_main", pd.DataFrame())
result_cmp  = st.session_state.get("result_cmp",  pd.DataFrame())
has_cmp     = st.session_state.get("cmp_active", False) and not result_cmp.empty

if result_main.empty:
    st.warning("Brak danych do wyświetlenia.")
    st.stop()

st.success("✅ Dane pobrane!")

# ── Global KPIs ────────────────────────────────
def cmp_val(col):
    return col_sum(result_cmp, col) if has_cmp else None

k1, k2, k3, k4, k5 = st.columns(5)
k1.markdown(kpi("Sesje",        col_sum(result_main,"Sesje"),        cmp_val("Sesje"),        dec=0), unsafe_allow_html=True)
k2.markdown(kpi("Transakcje",   col_sum(result_main,"Transakcje"),   cmp_val("Transakcje"),   dec=0), unsafe_allow_html=True)
k3.markdown(kpi("Przychód",     col_sum(result_main,"Przychód"),     cmp_val("Przychód"),     dec=2), unsafe_allow_html=True)
k4.markdown(kpi("AdCost (ADS)", col_sum(result_main,"AdCost (GA4)"), cmp_val("AdCost (GA4)"),dec=2, reverse=True), unsafe_allow_html=True)
k5.markdown(kpi("Spend Social", col_sum(result_main,"Spend (BQ)"),   cmp_val("Spend (BQ)"),  dec=2, reverse=True), unsafe_allow_html=True)

# ── Inline filters ────────────────────────────
st.markdown('<div class="sec">Filtry</div>', unsafe_allow_html=True)
f1, f2, f3, f4 = st.columns([1,1,1,2])
sec_filter  = f1.selectbox("Sekcja:",  ["Wszystkie"] + sorted(result_main["Sekcja"].dropna().unique()))
mpk_filter  = f2.selectbox("MPK:",     ["Wszystkie"] + sorted(result_main["MPK"].dropna().unique()))
med_filter  = f3.selectbox("Medium:",  ["Wszystkie"] + sorted(result_main["Medium"].dropna().unique()))
camp_search = f4.text_input("Szukaj kampanii:", placeholder="fragment nazwy…")

def apply_filters(df):
    if df is None or df.empty:
        return df
    if sec_filter  != "Wszystkie": df = df[df["Sekcja"]  == sec_filter]
    if mpk_filter  != "Wszystkie": df = df[df["MPK"]     == mpk_filter]
    if med_filter  != "Wszystkie": df = df[df["Medium"]  == med_filter]
    if camp_search: df = df[df["CampaignName"].str.contains(camp_search, case=False, na=False)]
    return df

filtered      = apply_filters(result_main.copy())
filtered_cmp  = apply_filters(result_cmp.copy()) if has_cmp else pd.DataFrame()

# ── Section tabs ──────────────────────────────
SECTION_ORDER = ["Wszystkie","ADS","Facebook","TikTok","Other"]
available_secs = [s for s in SECTION_ORDER
                  if s == "Wszystkie" or s in result_main["Sekcja"].unique()]
tabs = st.tabs(available_secs)

NUM_CFG = {
    "Przychód":     st.column_config.NumberColumn(format="%.2f"),
    "AdCost (GA4)": st.column_config.NumberColumn(format="%.2f"),
    "Spend (BQ)":   st.column_config.NumberColumn(format="%.2f"),
    "Sesje":        st.column_config.NumberColumn(format="%.0f"),
    "Transakcje":   st.column_config.NumberColumn(format="%.0f"),
}

for tab, sec in zip(tabs, available_secs):
    with tab:
        df_sec     = filtered[filtered["Sekcja"] == sec] if sec != "Wszystkie" else filtered
        df_sec_cmp = (filtered_cmp[filtered_cmp["Sekcja"] == sec]
                      if (sec != "Wszystkie" and has_cmp)
                      else (filtered_cmp if has_cmp else pd.DataFrame()))

        if df_sec.empty:
            st.info("Brak danych.")
            continue

        # Section KPIs
        sk1, sk2, sk3, sk4, sk5 = st.columns(5)
        def scmp(col):
            return col_sum(df_sec_cmp, col) if has_cmp else None
        sk1.markdown(kpi("Sesje",        col_sum(df_sec,"Sesje"),        scmp("Sesje"),        dec=0), unsafe_allow_html=True)
        sk2.markdown(kpi("Transakcje",   col_sum(df_sec,"Transakcje"),   scmp("Transakcje"),   dec=0), unsafe_allow_html=True)
        sk3.markdown(kpi("Przychód",     col_sum(df_sec,"Przychód"),     scmp("Przychód"),     dec=2), unsafe_allow_html=True)
        sk4.markdown(kpi("AdCost (GA4)", col_sum(df_sec,"AdCost (GA4)"), scmp("AdCost (GA4)"),dec=2, reverse=True), unsafe_allow_html=True)
        sk5.markdown(kpi("Spend (BQ)",   col_sum(df_sec,"Spend (BQ)"),   scmp("Spend (BQ)"),  dec=2, reverse=True), unsafe_allow_html=True)

        # Table
        if has_cmp and not df_sec_cmp.empty:
            JOIN_KEYS = ["MPK","Sekcja","Source","Medium","CampaignName"]
            merged = df_sec.merge(
                df_sec_cmp[JOIN_KEYS + ["Sesje","Transakcje","Przychód","AdCost (GA4)","Spend (BQ)"]].rename(columns={
                    "Sesje":        "Sesje_p",
                    "Transakcje":   "Trans_p",
                    "Przychód":     "Przychód_p",
                    "AdCost (GA4)": "AdCost_p",
                    "Spend (BQ)":   "Spend_p",
                }),
                on=JOIN_KEYS, how="left"
            ).fillna(0)
            merged["Δ Sesje"]    = merged["Sesje"]    - merged["Sesje_p"]
            merged["Δ Przychód"] = merged["Przychód"] - merged["Przychód_p"]
            merged["% Przychód"] = merged.apply(
                lambda r: r["Δ Przychód"] / r["Przychód_p"] * 100 if r["Przychód_p"] != 0 else 0, axis=1
            )
            merged["Δ Spend"]    = merged["Spend (BQ)"] - merged["Spend_p"]

            show_cols = ["MPK","Brand","Sekcja","Source","Medium","CampaignName",
                         "Sesje","Sesje_p","Δ Sesje",
                         "Transakcje","Trans_p",
                         "Przychód","Przychód_p","Δ Przychód","% Przychód",
                         "AdCost (GA4)","AdCost_p",
                         "Spend (BQ)","Spend_p","Δ Spend"]
            col_cfg = {
                **NUM_CFG,
                "Sesje_p":    st.column_config.NumberColumn(label="Sesje (cmp)",        format="%.0f"),
                "Δ Sesje":    st.column_config.NumberColumn(format="%.0f"),
                "Trans_p":    st.column_config.NumberColumn(label="Transakcje (cmp)",   format="%.0f"),
                "Przychód_p": st.column_config.NumberColumn(label="Przychód (cmp)",     format="%.2f"),
                "Δ Przychód": st.column_config.NumberColumn(format="%.2f"),
                "% Przychód": st.column_config.NumberColumn(format="%.1f%%"),
                "AdCost_p":   st.column_config.NumberColumn(label="AdCost (cmp)",       format="%.2f"),
                "Spend_p":    st.column_config.NumberColumn(label="Spend (cmp)",        format="%.2f"),
                "Δ Spend":    st.column_config.NumberColumn(format="%.2f"),
            }
            st.dataframe(merged[show_cols], use_container_width=True, height=460, column_config=col_cfg)
        else:
            st.dataframe(df_sec, use_container_width=True, height=460, column_config=NUM_CFG)

        st.caption(
            f"Wierszy: {len(df_sec):,}  |  "
            f"Przychód: {col_sum(df_sec,'Przychód'):,.2f}  |  "
            f"Spend: {col_sum(df_sec,'Spend (BQ)'):,.2f}  |  "
            f"AdCost: {col_sum(df_sec,'AdCost (GA4)'):,.2f}"
        )
