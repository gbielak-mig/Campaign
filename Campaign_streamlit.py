import streamlit as st
import pandas as pd
import re
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, FilterExpression,
    Filter, FilterExpressionList,
)
from google.cloud import bigquery
from datetime import datetime, timedelta, date

# ─────────────────────────────────────────────
# KONFIGURACJA
# ─────────────────────────────────────────────
st.set_page_config(page_title="GA4 + Social Dashboard", layout="wide", page_icon="📊")

GA4_DIMENSIONS = ["sessionSource", "sessionMedium", "sessionCampaignName"]
GA4_METRICS    = ["sessions", "transactions", "totalRevenue", "advertiserAdCost"]

PLATFORM_FILTERS = {
    "Web": ["WEB"],
    "App": ["IOS", "ANDROID"],
}

# ─────────────────────────────────────────────
# STYLE
# ─────────────────────────────────────────────
st.markdown("""
<style>
.metric-card {
    background: var(--color-background-secondary);
    border-radius: 8px;
    padding: 14px 16px;
    margin-bottom: 8px;
}
.metric-label {
    font-size: 12px;
    color: var(--color-text-secondary);
    margin-bottom: 4px;
}
.metric-value {
    font-size: 22px;
    font-weight: 500;
    color: var(--color-text-primary);
}
.metric-compare {
    font-size: 12px;
    margin-top: 2px;
}
.compare-pos { color: var(--color-text-success); }
.compare-neg { color: var(--color-text-danger); }
.compare-neu { color: var(--color-text-secondary); }
.section-header {
    font-size: 16px;
    font-weight: 500;
    color: var(--color-text-primary);
    margin: 24px 0 12px;
    padding-bottom: 6px;
    border-bottom: 0.5px solid var(--color-border-tertiary);
}
div[data-testid="stDataFrame"] {
    border: 0.5px solid var(--color-border-tertiary);
    border-radius: 8px;
    overflow: hidden;
}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# MAPOWANIE MPK
# ─────────────────────────────────────────────
def build_property_mapping():
    rows = []
    for mpk, vals in st.secrets["ga4_properties"].items():
        ga4_id   = int(vals[0])
        brand    = vals[1]
        currency = vals[2] if len(vals) > 2 else ""
        tt_alias = vals[3] if len(vals) > 3 else ""
        rows.append({"MPK": mpk, "ID_GA4": ga4_id, "Brand": brand,
                     "Currency": currency, "TT_Alias": tt_alias})
    return pd.DataFrame(rows)

property_mapping = build_property_mapping()

tt_alias_to_mpk = {
    row["TT_Alias"]: row["MPK"]
    for _, row in property_mapping.iterrows()
    if row["TT_Alias"]
}

META_ALIAS_TO_MPK = {
    "50PL": "S501", "BS": "S514", "SPL": "S500", "SDE": "G500",
    "SCZ": "CZ50", "SSK": "SK50", "SLT": "LT50", "SRO": "RO50",
    "SYM": "S502", "TBL": "S507", "JDPL": "S512", "JDRO": "RO55",
    "JDSK": "SK52", "JDHU": "HU52", "JDLT": "LT52", "JDBG": "BG52",
    "JDCZ": "CZ55", "JDUA": "UA52", "JDHR": "HR52",
}

def extract_meta_alias(campaign_name: str) -> str:
    if not campaign_name:
        return ""
    return campaign_name.split("-")[0].strip().upper()

# ─────────────────────────────────────────────
# KROK 1 – HASŁO
# ─────────────────────────────────────────────
if not st.session_state.get("authenticated"):
    st.title("🔐 GA4 + Social Dashboard")
    pwd = st.text_input("Hasło dostępu:", type="password")
    if st.button("Zaloguj", use_container_width=True):
        if pwd == st.secrets["app"]["password"]:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("❌ Błędne hasło!")
    st.stop()

# ─────────────────────────────────────────────
# KLIENTY
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
# POMOCNICZE
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

def fmt_num(val, is_currency=False, currency=""):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    if is_currency:
        return f"{val:,.2f} {currency}".strip()
    if isinstance(val, float) and val == int(val):
        return f"{int(val):,}"
    return f"{val:,.2f}"

def compare_html(current, previous, is_currency=False, currency="", reverse=False):
    """Returns HTML with value and comparison delta in parentheses."""
    curr_str = fmt_num(current, is_currency, currency)
    if previous is None or previous == 0:
        return f'<span>{curr_str}</span>'
    delta = current - previous
    pct   = (delta / previous) * 100 if previous != 0 else 0
    sign  = "+" if delta >= 0 else ""
    # reverse: lower = better (e.g. cost)
    good  = (delta >= 0) if not reverse else (delta <= 0)
    cls   = "compare-pos" if good else "compare-neg"
    if abs(pct) < 0.05:
        cls = "compare-neu"
    prev_str  = fmt_num(previous, is_currency, currency)
    delta_str = fmt_num(abs(delta), is_currency, currency)
    return (
        f'<span>{curr_str}</span> '
        f'<span class="metric-compare {cls}">({sign}{delta_str}, {sign}{pct:.1f}%)</span>'
    )

# ─────────────────────────────────────────────
# GA4
# ─────────────────────────────────────────────
def build_platform_filter(platform_values):
    if not platform_values:
        return None
    exprs = [
        FilterExpression(filter=Filter(
            field_name="platform",
            string_filter=Filter.StringFilter(
                match_type=Filter.StringFilter.MatchType.EXACT,
                value=pv, case_sensitive=False,
            ),
        ))
        for pv in platform_values
    ]
    return exprs[0] if len(exprs) == 1 else FilterExpression(
        or_group=FilterExpressionList(expressions=exprs)
    )

def get_ga4_data(property_id, start_date, end_date, platform_filter_expr=None):
    try:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in GA4_DIMENSIONS],
            metrics=[Metric(name=m) for m in GA4_METRICS],
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            dimension_filter=platform_filter_expr,
        )
        response = ga4_client.run_report(request)
        rows = []
        for row in response.rows:
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
        st.warning(f"GA4 błąd dla ID {property_id}: {e}")
        return pd.DataFrame(columns=GA4_DIMENSIONS + GA4_METRICS)

# ─────────────────────────────────────────────
# META
# ─────────────────────────────────────────────
def get_meta_data(start_date, end_date):
    query = f"""
        SELECT CampaignName, DateStart, AdCampaignId, Clicks, Spend
        FROM `facebook-423312.meta.AdInsights`
        WHERE DateStart BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        df = bq_client.query(query).to_dataframe()
    except Exception as e:
        st.warning(f"Meta BQ błąd: {e}")
        return pd.DataFrame()
    if df.empty:
        return df
    df["_alias"] = df["CampaignName"].astype(str).apply(extract_meta_alias)
    df["MPK"]    = df["_alias"].map(META_ALIAS_TO_MPK).fillna("")
    df["source"] = "Meta"
    df = df.drop(columns=["_alias"])
    df["Clicks"]    = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
    df["Spend"]     = pd.to_numeric(df["Spend"],  errors="coerce").fillna(0)
    df["DateStart"] = pd.to_datetime(df["DateStart"], errors="coerce").dt.date
    return df

# ─────────────────────────────────────────────
# TIKTOK
# ─────────────────────────────────────────────
def get_tiktok_data(start_date, end_date):
    query = f"""
        SELECT advertiser_name, stream_name, date, campaign_id, campaign_name, spend
        FROM `facebook-423312.tiktok_tik_tok`
        WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """
    try:
        df = bq_client.query(query).to_dataframe()
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
    df["source"] = "TikTok"
    df["spend"]  = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
    df["date"]   = pd.to_datetime(df["date"], errors="coerce").dt.date
    return df

# ─────────────────────────────────────────────
# KAMPANIE ŁĄCZONE (BQ spend + GA4 revenue)
# ─────────────────────────────────────────────
def get_campaign_join(
    meta_df, tiktok_df,
    ga4_combined_df,
    selected_mpk_set,
):
    """
    Łączy kampanie z BQ (spend) z danymi GA4 (revenue) po sessionCampaignName.
    Zwraca DataFrame z kolumnami: source, MPK, campaign_name, spend, sessions,
    transactions, revenue_ga4, adcost_ga4, roas
    """
    frames = []

    # Meta
    if meta_df is not None and not meta_df.empty:
        m = meta_df[["MPK", "CampaignName", "Spend"]].copy()
        m = m.rename(columns={"CampaignName": "campaign_name", "Spend": "spend"})
        m["source"] = "Meta"
        frames.append(m)

    # TikTok
    if tiktok_df is not None and not tiktok_df.empty:
        t = tiktok_df[["MPK", "campaign_name", "spend"]].copy()
        t["source"] = "TikTok"
        frames.append(t)

    if not frames:
        return pd.DataFrame()

    social = pd.concat(frames, ignore_index=True)
    social = social.groupby(["source", "MPK", "campaign_name"], as_index=False)["spend"].sum()

    # GA4 per campaign
    if ga4_combined_df is not None and not ga4_combined_df.empty:
        ga4_camp = (
            ga4_combined_df
            .groupby(["MPK", "sessionCampaignName"], as_index=False)
            .agg(
                sessions=("sessions", "sum"),
                transactions=("transactions", "sum"),
                revenue_ga4=("totalRevenue", "sum"),
                adcost_ga4=("advertiserAdCost", "sum"),
            )
            .rename(columns={"sessionCampaignName": "campaign_name"})
        )
        result = social.merge(ga4_camp, on=["MPK", "campaign_name"], how="left")
    else:
        result = social.copy()
        result["sessions"] = 0
        result["transactions"] = 0
        result["revenue_ga4"] = 0.0
        result["adcost_ga4"] = 0.0

    result = result.fillna(0)
    result["roas"] = result.apply(
        lambda r: r["revenue_ga4"] / r["spend"] if r["spend"] > 0 else 0, axis=1
    )

    if selected_mpk_set:
        result = result[result["MPK"].isin(selected_mpk_set)]

    return result.sort_values("spend", ascending=False).reset_index(drop=True)

# ─────────────────────────────────────────────
# UI – SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.title("📊 GA4 + Social")
    st.markdown("---")

    st.subheader("📡 Źródło danych")
    data_source = st.radio(
        "Wybierz źródło:",
        ["GA4", "Meta", "TikTok", "Social (Meta + TikTok)", "Wszystko (GA4 + Social)"],
        index=4,
    )
    use_ga4    = data_source in ("GA4", "Wszystko (GA4 + Social)")
    use_meta   = data_source in ("Meta", "Social (Meta + TikTok)", "Wszystko (GA4 + Social)")
    use_tiktok = data_source in ("TikTok", "Social (Meta + TikTok)", "Wszystko (GA4 + Social)")

    st.markdown("---")

    if use_ga4:
        st.subheader("📱 Platforma GA4")
        platform_choice = st.radio(
            "Źródło danych GA4:",
            ["Web", "App", "Web + App"],
            index=2,
        )
        st.markdown("---")
    else:
        platform_choice = "Web + App"

    st.subheader("🏬 Sklep")
    brand_options   = sorted(property_mapping["Brand"].unique().tolist())
    selected_brands = st.multiselect("Brand (brak = wszystkie):", options=brand_options, default=[])

    if selected_brands:
        filtered_map = property_mapping[property_mapping["Brand"].isin(selected_brands)]
    else:
        filtered_map = property_mapping.copy()

    mpk_options   = sorted(filtered_map["MPK"].tolist())
    selected_mpks = st.multiselect("MPK (brak = wszystkie):", options=mpk_options, default=[])

    if selected_mpks:
        filtered_map = filtered_map[filtered_map["MPK"].isin(selected_mpks)]

    st.caption(f"Wybrano {len(filtered_map)} sklep(ów)")
    selected_mpk_set = set(filtered_map["MPK"].tolist())
    st.markdown("---")

    st.subheader("📅 Zakres dat")
    preset_label = st.selectbox("Zakres:", options=list(DATE_PRESETS.keys()), index=0)
    preset_days  = DATE_PRESETS[preset_label]

    if preset_days is not None:
        start_date = yesterday - timedelta(days=preset_days - 1)
        end_date   = yesterday
        st.caption(f"📆 {start_date} → {end_date}")
    else:
        start_date = st.date_input("Od:", value=yesterday - timedelta(days=6), max_value=yesterday)
        end_date   = st.date_input("Do:", value=yesterday, max_value=yesterday)
        if start_date > end_date:
            st.error("Data 'Od' musi być wcześniej niż 'Do'!")

    st.markdown("---")
    st.subheader("🔁 Porównanie")
    delta_days = (end_date - start_date).days + 1
    compare_mode = st.selectbox(
        "Porównaj z:",
        ["Tydzień wstecz", "Poprzedni okres", "Rok wcześniej", "Własny zakres", "Brak"],
        index=0,
    )
    cmp_start = cmp_end = None

    if compare_mode == "Tydzień wstecz":
        cmp_start = start_date - timedelta(weeks=1)
        cmp_end   = end_date   - timedelta(weeks=1)
        st.caption(f"Okres: {cmp_start} → {cmp_end}")
    elif compare_mode == "Poprzedni okres":
        cmp_end   = start_date - timedelta(days=1)
        cmp_start = cmp_end - timedelta(days=delta_days - 1)
        st.caption(f"Okres: {cmp_start} → {cmp_end}")
    elif compare_mode == "Rok wcześniej":
        cmp_start = start_date - timedelta(weeks=52)
        cmp_end   = end_date   - timedelta(weeks=52)
        st.caption(f"Okres: {cmp_start} → {cmp_end}")
    elif compare_mode == "Własny zakres":
        cmp_start = st.date_input("Porównaj Od:", value=start_date - timedelta(days=delta_days))
        cmp_end   = st.date_input("Porównaj Do:", value=start_date - timedelta(days=1))

    st.markdown("---")
    run_button = st.button("🚀 URUCHOM", use_container_width=True, type="primary")

# ─────────────────────────────────────────────
# GŁÓWNA TREŚĆ
# ─────────────────────────────────────────────
st.title("📊 GA4 + Social Dashboard")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Sklepów",   len(filtered_map))
col2.metric("Źródło",    data_source)
col3.metric("Platforma", platform_choice if use_ga4 else "—")
col4.metric("Zakres",    f"{start_date} → {end_date}")

if not run_button:
    if "last_results" in st.session_state:
        st.info("Wyniki z poprzedniego uruchomienia (kliknij 🚀 aby odświeżyć).")
        _render_results = True
    else:
        st.info("Ustaw parametry w panelu bocznym i kliknij **🚀 URUCHOM**.")
        st.stop()
else:
    _render_results = False

if start_date > end_date:
    st.error("Błędny zakres dat!")
    st.stop()
if filtered_map.empty:
    st.error("Nie wybrano żadnych sklepów!")
    st.stop()

# ─────────────────────────────────────────────
# POBIERANIE DANYCH
# ─────────────────────────────────────────────
if run_button:
    combined_ga4 = pd.DataFrame()
    combined_ga4_cmp = pd.DataFrame()
    ga4_per_mpk = {}
    ga4_per_mpk_cmp = {}

    if use_ga4:
        if platform_choice == "Web + App":
            platform_expr = None
        elif platform_choice == "Web":
            platform_expr = build_platform_filter(PLATFORM_FILTERS["Web"])
        else:
            platform_expr = build_platform_filter(PLATFORM_FILTERS["App"])

        progress_bar = st.progress(0)
        status_text  = st.empty()

        for i, (_, row) in enumerate(filtered_map.iterrows()):
            status_text.write(f"⏳ GA4 – **{row['MPK']}** ({row['Brand']})…")
            df_main = get_ga4_data(row["ID_GA4"], start_date, end_date, platform_expr)
            df_cmp  = (
                get_ga4_data(row["ID_GA4"], cmp_start, cmp_end, platform_expr)
                if cmp_start else None
            )

            def enrich_ga4(df, label):
                if df is not None and not df.empty:
                    df = df.copy()
                    df["MPK"]        = row["MPK"]
                    df["Brand"]      = row["Brand"]
                    df["Platforma"]  = platform_choice
                    df["date_range"] = label
                return df

            lbl     = f"{start_date.strftime('%d%m%Y')}_{end_date.strftime('%d%m%Y')}"
            lbl_cmp = (f"{cmp_start.strftime('%d%m%Y')}_{cmp_end.strftime('%d%m%Y')}" if cmp_start else None)

            df_main = enrich_ga4(df_main, lbl)
            df_cmp  = enrich_ga4(df_cmp,  lbl_cmp)

            if df_main is not None and not df_main.empty:
                ga4_per_mpk[row["MPK"]] = df_main
                combined_ga4 = pd.concat([combined_ga4, df_main], ignore_index=True)
            if df_cmp is not None and not df_cmp.empty:
                ga4_per_mpk_cmp[row["MPK"]] = df_cmp
                combined_ga4_cmp = pd.concat([combined_ga4_cmp, df_cmp], ignore_index=True)

            progress_bar.progress((i + 1) / len(filtered_map))

        status_text.empty()
        progress_bar.empty()

    combined_meta = pd.DataFrame()
    combined_meta_cmp = pd.DataFrame()
    if use_meta:
        with st.spinner("⏳ Pobieranie danych Meta…"):
            df_meta = get_meta_data(start_date, end_date)
            if not df_meta.empty and selected_mpk_set:
                df_meta = df_meta[df_meta["MPK"].isin(selected_mpk_set)]
            combined_meta = df_meta
            if cmp_start:
                df_meta_cmp = get_meta_data(cmp_start, cmp_end)
                if not df_meta_cmp.empty and selected_mpk_set:
                    df_meta_cmp = df_meta_cmp[df_meta_cmp["MPK"].isin(selected_mpk_set)]
                combined_meta_cmp = df_meta_cmp

    combined_tiktok = pd.DataFrame()
    combined_tiktok_cmp = pd.DataFrame()
    if use_tiktok:
        with st.spinner("⏳ Pobieranie danych TikTok…"):
            df_tt = get_tiktok_data(start_date, end_date)
            if not df_tt.empty and selected_mpk_set:
                df_tt = df_tt[df_tt["MPK"].isin(selected_mpk_set)]
            combined_tiktok = df_tt
            if cmp_start:
                df_tt_cmp = get_tiktok_data(cmp_start, cmp_end)
                if not df_tt_cmp.empty and selected_mpk_set:
                    df_tt_cmp = df_tt_cmp[df_tt_cmp["MPK"].isin(selected_mpk_set)]
                combined_tiktok_cmp = df_tt_cmp

    # Campaign join
    campaign_df = pd.DataFrame()
    campaign_cmp_df = pd.DataFrame()
    if use_meta or use_tiktok:
        campaign_df = get_campaign_join(
            combined_meta, combined_tiktok, combined_ga4, selected_mpk_set
        )
        if cmp_start:
            campaign_cmp_df = get_campaign_join(
                combined_meta_cmp, combined_tiktok_cmp, combined_ga4_cmp, selected_mpk_set
            )

    st.session_state["last_results"] = {
        "combined_ga4":        combined_ga4,
        "combined_ga4_cmp":    combined_ga4_cmp,
        "combined_meta":       combined_meta,
        "combined_meta_cmp":   combined_meta_cmp,
        "combined_tiktok":     combined_tiktok,
        "combined_tiktok_cmp": combined_tiktok_cmp,
        "campaign_df":         campaign_df,
        "campaign_cmp_df":     campaign_cmp_df,
        "cmp_start":           cmp_start,
        "cmp_end":             cmp_end,
        "use_ga4":             use_ga4,
        "use_meta":            use_meta,
        "use_tiktok":          use_tiktok,
    }

# ─────────────────────────────────────────────
# RENDER WYNIKÓW
# ─────────────────────────────────────────────
res = st.session_state.get("last_results", {})
if not res:
    st.stop()

combined_ga4        = res.get("combined_ga4",        pd.DataFrame())
combined_ga4_cmp    = res.get("combined_ga4_cmp",    pd.DataFrame())
combined_meta       = res.get("combined_meta",       pd.DataFrame())
combined_meta_cmp   = res.get("combined_meta_cmp",   pd.DataFrame())
combined_tiktok     = res.get("combined_tiktok",     pd.DataFrame())
combined_tiktok_cmp = res.get("combined_tiktok_cmp", pd.DataFrame())
campaign_df         = res.get("campaign_df",         pd.DataFrame())
campaign_cmp_df     = res.get("campaign_cmp_df",     pd.DataFrame())
_cmp_start          = res.get("cmp_start")
_use_ga4            = res.get("use_ga4",    use_ga4)
_use_meta           = res.get("use_meta",   use_meta)
_use_tiktok         = res.get("use_tiktok", use_tiktok)

has_cmp = _cmp_start is not None

st.success("✅ Dane pobrane!")

# ── HELPER: metric cards ─────────────────────
def metric_card(label, current_val, prev_val=None, is_currency=False, currency="", reverse=False):
    val_html = compare_html(current_val, prev_val, is_currency, currency, reverse) if has_cmp and prev_val is not None else f"<span>{fmt_num(current_val, is_currency, currency)}</span>"
    return f"""
<div class="metric-card">
  <div class="metric-label">{label}</div>
  <div class="metric-value">{val_html}</div>
</div>"""

def safe_sum(df, col):
    if df is None or df.empty or col not in df.columns:
        return 0.0
    return float(df[col].sum())

def safe_sum_cmp(df, col):
    if not has_cmp:
        return None
    return safe_sum(df, col)

# ─────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────
tab_labels = []
if _use_ga4:            tab_labels.append("📊 GA4")
if _use_meta or _use_tiktok: tab_labels.append("📣 Social")
if (_use_meta or _use_tiktok) and _use_ga4: tab_labels.append("🔗 Kampanie")

tabs = st.tabs(tab_labels) if tab_labels else []
tab_map = {name: tab for name, tab in zip(tab_labels, tabs)}

# ══════════════════════════════════════════════
# TAB: GA4
# ══════════════════════════════════════════════
if "📊 GA4" in tab_map:
    with tab_map["📊 GA4"]:

        # KPI cards
        ga4_sessions     = safe_sum(combined_ga4, "sessions")
        ga4_transactions = safe_sum(combined_ga4, "transactions")
        ga4_revenue      = safe_sum(combined_ga4, "totalRevenue")
        ga4_adcost       = safe_sum(combined_ga4, "advertiserAdCost")

        ga4_sessions_c     = safe_sum_cmp(combined_ga4_cmp, "sessions")
        ga4_transactions_c = safe_sum_cmp(combined_ga4_cmp, "transactions")
        ga4_revenue_c      = safe_sum_cmp(combined_ga4_cmp, "totalRevenue")
        ga4_adcost_c       = safe_sum_cmp(combined_ga4_cmp, "advertiserAdCost")

        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(metric_card("Sesje", ga4_sessions, ga4_sessions_c), unsafe_allow_html=True)
        c2.markdown(metric_card("Transakcje", ga4_transactions, ga4_transactions_c), unsafe_allow_html=True)
        c3.markdown(metric_card("Przychód", ga4_revenue, ga4_revenue_c, is_currency=True), unsafe_allow_html=True)
        c4.markdown(metric_card("Ad Cost (GA4)", ga4_adcost, ga4_adcost_c, is_currency=True, reverse=True), unsafe_allow_html=True)

        # Tabela GA4 per source/medium/campaign
        st.markdown('<div class="section-header">Dane GA4 – źródło / medium / kampania</div>', unsafe_allow_html=True)

        if not combined_ga4.empty:
            ga4_agg = (
                combined_ga4
                .groupby(["sessionSource", "sessionMedium", "sessionCampaignName"], as_index=False)
                .agg(sessions=("sessions","sum"), transactions=("transactions","sum"),
                     totalRevenue=("totalRevenue","sum"), advertiserAdCost=("advertiserAdCost","sum"))
                .sort_values("sessions", ascending=False)
            )
            # Jeśli jest porównanie – dodaj kolumny delta
            if has_cmp and not combined_ga4_cmp.empty:
                ga4_agg_c = (
                    combined_ga4_cmp
                    .groupby(["sessionSource", "sessionMedium", "sessionCampaignName"], as_index=False)
                    .agg(sessions_c=("sessions","sum"), transactions_c=("transactions","sum"),
                         totalRevenue_c=("totalRevenue","sum"), advertiserAdCost_c=("advertiserAdCost","sum"))
                )
                ga4_agg = ga4_agg.merge(ga4_agg_c, on=["sessionSource","sessionMedium","sessionCampaignName"], how="left").fillna(0)
                for col, col_c in [("sessions","sessions_c"),("transactions","transactions_c"),
                                   ("totalRevenue","totalRevenue_c"),("advertiserAdCost","advertiserAdCost_c")]:
                    ga4_agg[f"{col}_delta"] = ga4_agg[col] - ga4_agg[col_c]
                    ga4_agg[f"{col}_pct"] = ga4_agg.apply(
                        lambda r: (r[col]-r[col_c])/r[col_c]*100 if r[col_c] != 0 else 0, axis=1
                    )
                # Format display
                display_cols = {
                    "sessionSource": "Źródło", "sessionMedium": "Medium",
                    "sessionCampaignName": "Kampania",
                    "sessions": "Sesje", "sessions_c": "Sesje (cmp)",
                    "sessions_delta": "Δ Sesje", "sessions_pct": "% Sesje",
                    "transactions": "Transakcje", "transactions_c": "Transakcje (cmp)",
                    "totalRevenue": "Przychód", "totalRevenue_c": "Przychód (cmp)",
                    "totalRevenue_delta": "Δ Przychód",
                }
                show_cols = ["sessionSource","sessionMedium","sessionCampaignName",
                             "sessions","sessions_c","sessions_pct",
                             "transactions","transactions_c",
                             "totalRevenue","totalRevenue_c","totalRevenue_delta"]
                ga4_disp = ga4_agg[show_cols].rename(columns=display_cols)
            else:
                ga4_disp = ga4_agg[["sessionSource","sessionMedium","sessionCampaignName",
                                     "sessions","transactions","totalRevenue","advertiserAdCost"]].rename(columns={
                    "sessionSource":"Źródło","sessionMedium":"Medium",
                    "sessionCampaignName":"Kampania","sessions":"Sesje",
                    "transactions":"Transakcje","totalRevenue":"Przychód","advertiserAdCost":"Ad Cost",
                })

            st.dataframe(ga4_disp, use_container_width=True, height=420,
                         column_config={
                             "Przychód": st.column_config.NumberColumn(format="%.2f"),
                             "Przychód (cmp)": st.column_config.NumberColumn(format="%.2f"),
                             "Δ Przychód": st.column_config.NumberColumn(format="%.2f"),
                             "% Sesje": st.column_config.NumberColumn(format="%.1f%%"),
                         })
            st.caption(f"Wierszy: {len(ga4_disp):,}")
        else:
            st.info("Brak danych GA4.")

        # Per MPK summary
        st.markdown('<div class="section-header">Podsumowanie per MPK</div>', unsafe_allow_html=True)
        if not combined_ga4.empty:
            mpk_agg = (
                combined_ga4
                .groupby(["MPK","Brand"], as_index=False)
                .agg(sessions=("sessions","sum"), transactions=("transactions","sum"),
                     totalRevenue=("totalRevenue","sum"), advertiserAdCost=("advertiserAdCost","sum"))
                .sort_values("totalRevenue", ascending=False)
            )
            if has_cmp and not combined_ga4_cmp.empty:
                mpk_agg_c = (
                    combined_ga4_cmp
                    .groupby(["MPK","Brand"], as_index=False)
                    .agg(sessions_c=("sessions","sum"), transactions_c=("transactions","sum"),
                         totalRevenue_c=("totalRevenue","sum"))
                )
                mpk_agg = mpk_agg.merge(mpk_agg_c, on=["MPK","Brand"], how="left").fillna(0)
                for col, col_c in [("sessions","sessions_c"),("transactions","transactions_c"),("totalRevenue","totalRevenue_c")]:
                    mpk_agg[f"{col}_pct"] = mpk_agg.apply(
                        lambda r, c=col, cc=col_c: (r[c]-r[cc])/r[cc]*100 if r[cc]!=0 else 0, axis=1
                    )
            st.dataframe(mpk_agg, use_container_width=True, height=320)

# ══════════════════════════════════════════════
# TAB: SOCIAL
# ══════════════════════════════════════════════
if "📣 Social" in tab_map:
    with tab_map["📣 Social"]:
        meta_spend   = safe_sum(combined_meta,   "Spend")
        tt_spend     = safe_sum(combined_tiktok, "spend")
        total_spend  = meta_spend + tt_spend

        meta_spend_c  = safe_sum_cmp(combined_meta_cmp,   "Spend")
        tt_spend_c    = safe_sum_cmp(combined_tiktok_cmp, "spend")
        total_spend_c = (meta_spend_c or 0) + (tt_spend_c or 0) if has_cmp else None

        c1, c2, c3 = st.columns(3)
        c1.markdown(metric_card("Spend łączny", total_spend, total_spend_c, is_currency=True, reverse=True), unsafe_allow_html=True)
        c2.markdown(metric_card("Spend Meta",   meta_spend,  meta_spend_c,  is_currency=True, reverse=True), unsafe_allow_html=True)
        c3.markdown(metric_card("Spend TikTok", tt_spend,    tt_spend_c,    is_currency=True, reverse=True), unsafe_allow_html=True)

        # Tabs Meta / TikTok
        social_tabs = []
        if _use_meta:   social_tabs.append("📘 Meta")
        if _use_tiktok: social_tabs.append("🎵 TikTok")

        st_tabs = st.tabs(social_tabs)
        stab_map = {n: t for n, t in zip(social_tabs, st_tabs)}

        if "📘 Meta" in stab_map:
            with stab_map["📘 Meta"]:
                if not combined_meta.empty:
                    meta_disp = (
                        combined_meta
                        .groupby(["MPK","CampaignName"], as_index=False)
                        .agg(Spend=("Spend","sum"), Clicks=("Clicks","sum"))
                        .sort_values("Spend", ascending=False)
                    )
                    if has_cmp and not combined_meta_cmp.empty:
                        meta_disp_c = (
                            combined_meta_cmp
                            .groupby(["MPK","CampaignName"], as_index=False)
                            .agg(Spend_c=("Spend","sum"))
                        )
                        meta_disp = meta_disp.merge(meta_disp_c, on=["MPK","CampaignName"], how="left").fillna(0)
                        meta_disp["Spend_delta"] = meta_disp["Spend"] - meta_disp["Spend_c"]
                        meta_disp["Spend_pct"]   = meta_disp.apply(
                            lambda r: (r["Spend"]-r["Spend_c"])/r["Spend_c"]*100 if r["Spend_c"]!=0 else 0, axis=1
                        )
                    st.dataframe(meta_disp, use_container_width=True, height=420,
                                 column_config={
                                     "Spend":       st.column_config.NumberColumn(format="%.2f"),
                                     "Spend_c":     st.column_config.NumberColumn(label="Spend (cmp)", format="%.2f"),
                                     "Spend_delta": st.column_config.NumberColumn(label="Δ Spend", format="%.2f"),
                                     "Spend_pct":   st.column_config.NumberColumn(label="% Spend", format="%.1f%%"),
                                 })
                    st.caption(f"Wierszy: {len(meta_disp):,}")
                else:
                    st.info("Brak danych Meta.")

        if "🎵 TikTok" in stab_map:
            with stab_map["🎵 TikTok"]:
                if not combined_tiktok.empty:
                    tt_disp = (
                        combined_tiktok
                        .groupby(["MPK","campaign_name"], as_index=False)
                        .agg(spend=("spend","sum"))
                        .sort_values("spend", ascending=False)
                    )
                    if has_cmp and not combined_tiktok_cmp.empty:
                        tt_disp_c = (
                            combined_tiktok_cmp
                            .groupby(["MPK","campaign_name"], as_index=False)
                            .agg(spend_c=("spend","sum"))
                        )
                        tt_disp = tt_disp.merge(tt_disp_c, on=["MPK","campaign_name"], how="left").fillna(0)
                        tt_disp["spend_delta"] = tt_disp["spend"] - tt_disp["spend_c"]
                        tt_disp["spend_pct"]   = tt_disp.apply(
                            lambda r: (r["spend"]-r["spend_c"])/r["spend_c"]*100 if r["spend_c"]!=0 else 0, axis=1
                        )
                    st.dataframe(tt_disp, use_container_width=True, height=420,
                                 column_config={
                                     "spend":       st.column_config.NumberColumn(format="%.2f"),
                                     "spend_c":     st.column_config.NumberColumn(label="Spend (cmp)", format="%.2f"),
                                     "spend_delta": st.column_config.NumberColumn(label="Δ Spend", format="%.2f"),
                                     "spend_pct":   st.column_config.NumberColumn(label="% Spend", format="%.1f%%"),
                                 })
                    st.caption(f"Wierszy: {len(tt_disp):,}")
                else:
                    st.info("Brak danych TikTok.")

# ══════════════════════════════════════════════
# TAB: KAMPANIE (BQ spend + GA4 revenue)
# ══════════════════════════════════════════════
if "🔗 Kampanie" in tab_map:
    with tab_map["🔗 Kampanie"]:
        st.markdown("""
        Kampanie łączą **spend z BQ** (Meta / TikTok) z **przychodem i sesjami z GA4**
        dopasowanymi po nazwie kampanii (`sessionCampaignName`).
        """)

        if not campaign_df.empty:
            # KPI
            camp_spend    = campaign_df["spend"].sum()
            camp_revenue  = campaign_df["revenue_ga4"].sum()
            camp_roas_avg = camp_revenue / camp_spend if camp_spend > 0 else 0
            camp_trans    = campaign_df["transactions"].sum()

            camp_spend_c   = campaign_cmp_df["spend"].sum()   if has_cmp and not campaign_cmp_df.empty else None
            camp_revenue_c = campaign_cmp_df["revenue_ga4"].sum() if has_cmp and not campaign_cmp_df.empty else None
            camp_roas_c    = camp_revenue_c / camp_spend_c if (camp_spend_c and camp_spend_c>0) else None
            camp_trans_c   = campaign_cmp_df["transactions"].sum() if has_cmp and not campaign_cmp_df.empty else None

            c1, c2, c3, c4 = st.columns(4)
            c1.markdown(metric_card("Spend (BQ)", camp_spend, camp_spend_c, is_currency=True, reverse=True), unsafe_allow_html=True)
            c2.markdown(metric_card("Przychód (GA4)", camp_revenue, camp_revenue_c, is_currency=True), unsafe_allow_html=True)
            c3.markdown(metric_card("ROAS", camp_roas_avg, camp_roas_c), unsafe_allow_html=True)
            c4.markdown(metric_card("Transakcje", camp_trans, camp_trans_c), unsafe_allow_html=True)

            st.markdown('<div class="section-header">Kampanie – szczegóły</div>', unsafe_allow_html=True)

            # Filtry inline
            fi1, fi2 = st.columns(2)
            with fi1:
                src_filter = st.multiselect("Źródło:", options=sorted(campaign_df["source"].unique()), default=[])
            with fi2:
                mpk_filter = st.multiselect("MPK:", options=sorted(campaign_df["MPK"].unique()), default=[])

            disp = campaign_df.copy()
            if src_filter: disp = disp[disp["source"].isin(src_filter)]
            if mpk_filter: disp = disp[disp["MPK"].isin(mpk_filter)]

            disp_renamed = disp.rename(columns={
                "source": "Źródło", "MPK": "MPK", "campaign_name": "Kampania",
                "spend": "Spend (BQ)", "sessions": "Sesje", "transactions": "Transakcje",
                "revenue_ga4": "Przychód (GA4)", "adcost_ga4": "AdCost (GA4)", "roas": "ROAS",
            })

            st.dataframe(
                disp_renamed,
                use_container_width=True,
                height=480,
                column_config={
                    "Spend (BQ)":     st.column_config.NumberColumn(format="%.2f"),
                    "Przychód (GA4)": st.column_config.NumberColumn(format="%.2f"),
                    "AdCost (GA4)":   st.column_config.NumberColumn(format="%.2f"),
                    "ROAS":           st.column_config.NumberColumn(format="%.2f"),
                }
            )
            st.caption(f"Wierszy: {len(disp):,}  |  Spend łączny: {disp['spend'].sum():,.2f}  |  Przychód łączny: {disp['revenue_ga4'].sum():,.2f}")

            # Per source summary
            st.markdown('<div class="section-header">Podsumowanie per źródło</div>', unsafe_allow_html=True)
            src_sum = (
                campaign_df
                .groupby("source", as_index=False)
                .agg(
                    Spend=("spend","sum"), Sessions=("sessions","sum"),
                    Transactions=("transactions","sum"), Revenue=("revenue_ga4","sum")
                )
            )
            src_sum["ROAS"] = src_sum.apply(lambda r: r["Revenue"]/r["Spend"] if r["Spend"]>0 else 0, axis=1)
            if has_cmp and not campaign_cmp_df.empty:
                src_sum_c = (
                    campaign_cmp_df
                    .groupby("source", as_index=False)
                    .agg(Spend_c=("spend","sum"), Revenue_c=("revenue_ga4","sum"))
                )
                src_sum = src_sum.merge(src_sum_c, on="source", how="left").fillna(0)
            st.dataframe(src_sum, use_container_width=True,
                         column_config={
                             "Spend":     st.column_config.NumberColumn(format="%.2f"),
                             "Revenue":   st.column_config.NumberColumn(format="%.2f"),
                             "Spend_c":   st.column_config.NumberColumn(label="Spend (cmp)", format="%.2f"),
                             "Revenue_c": st.column_config.NumberColumn(label="Revenue (cmp)", format="%.2f"),
                             "ROAS":      st.column_config.NumberColumn(format="%.2f"),
                         })
        else:
            st.info("Brak danych kampanii (Meta/TikTok). Upewnij się, że dane BQ są dostępne.")
