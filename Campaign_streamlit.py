import streamlit as st
import pandas as pd
import io
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
st.set_page_config(page_title="GA4 + Social Exporter", layout="wide", page_icon="📊")

GA4_DIMENSIONS = ["sessionSource", "sessionMedium", "sessionCampaignName"]
GA4_METRICS    = ["sessions", "transactions", "totalRevenue", "advertiserAdCost"]

PLATFORM_FILTERS = {
    "Web": ["WEB"],
    "App": ["IOS", "ANDROID"],
}

# ─────────────────────────────────────────────
# MAPOWANIE MPK ← secrets
# ─────────────────────────────────────────────
# secrets["ga4_properties"] ma format:
#   S501 = ["224194612", "CB",      "PLN", "50PL"]   # alias TikTok / skrót Meta w ostatnim polu
# LUB (stary format bez aliasu) – obsługujemy oba
#
# Budujemy:
#   property_mapping – DataFrame: MPK | ID_GA4 | Brand | Currency | TT_Alias | Meta_Alias
#
# Meta alias = pierwsza część campaign_name przed "-", np. "SYM" → mapuje na S502
# TT  alias  = advertiser_name, np. "50PL"             → mapuje na S501

def build_property_mapping():
    rows = []
    for mpk, vals in st.secrets["ga4_properties"].items():
        # vals[0]=GA4 ID, vals[1]=Brand, vals[2]=Currency, vals[3]=TT/alias (opcjonalne)
        ga4_id   = int(vals[0])
        brand    = vals[1]
        currency = vals[2] if len(vals) > 2 else ""
        tt_alias = vals[3] if len(vals) > 3 else ""
        rows.append({"MPK": mpk, "ID_GA4": ga4_id, "Brand": brand,
                     "Currency": currency, "TT_Alias": tt_alias})
    return pd.DataFrame(rows)

property_mapping = build_property_mapping()

# Słownik: TT_Alias → MPK  (np. "50PL" → "S501")
tt_alias_to_mpk = {
    row["TT_Alias"]: row["MPK"]
    for _, row in property_mapping.iterrows()
    if row["TT_Alias"]
}

# Słownik MPK → lista Meta aliasów z campaign_name
# Zakładamy Meta alias = ostatni człon kodu przed "-" równy MPK-skrótowi
# Mapowanie ręczne z treści zadania (Meta alias → MPK):
META_ALIAS_TO_MPK = {
    # z secrets: alias (kod sklepu z campaign_name) → MPK
    "50PL": "S501",
    "BS":   "S514",
    "SPL":  "S500",
    "SDE":  "G500",
    "SCZ":  "CZ50",
    "SSK":  "SK50",
    "SLT":  "LT50",
    "SRO":  "RO50",
    "SYM":  "S502",
    "TBL":  "S507",
    "JDPL": "S512",
    "JDRO": "RO55",
    "JDSK": "SK52",
    "JDHU": "HU52",
    "JDLT": "LT52",
    "JDBG": "BG52",
    "JDCZ": "CZ55",
    "JDUA": "UA52",
    "JDHR": "HR52",
}

def extract_meta_alias(campaign_name: str) -> str:
    """Wyciąga kod sklepu z nazwy kampanii Meta, np. 'SYM-ECPF-SLS-DPA-300124' → 'SYM'."""
    if not campaign_name:
        return ""
    return campaign_name.split("-")[0].strip().upper()

# ─────────────────────────────────────────────
# KROK 1 – HASŁO
# ─────────────────────────────────────────────
if not st.session_state.get("authenticated"):
    st.title("🔐 GA4 + Social Exporter")
    pwd = st.text_input("Hasło dostępu:", type="password")
    if st.button("Zaloguj", use_container_width=True):
        if pwd == st.secrets["app"]["password"]:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("❌ Błędne hasło!")
    st.stop()

# ─────────────────────────────────────────────
# KROK 2 – KLIENTY
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
        scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
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
# POMOCNICZE – DATY
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

# ─────────────────────────────────────────────
# POBIERANIE – GA4
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
# POBIERANIE – META (BQ)
# ─────────────────────────────────────────────
def get_meta_data(start_date, end_date):
    """
    Pobiera dane z Meta AdInsights w BQ i mapuje na MPK.
    Kolumny wynikowe: MPK, CampaignName, DateStart, AdCampaignId, Clicks, Spend, source
    """
    query = f"""
        SELECT
            CampaignName,
            DateStart,
            AdCampaignId,
            Clicks,
            Spend
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

    # Wyciągnij alias i zamapuj na MPK
    df["_alias"] = df["CampaignName"].astype(str).apply(extract_meta_alias)
    df["MPK"]    = df["_alias"].map(META_ALIAS_TO_MPK).fillna("")
    df["source"] = "Meta"
    df = df.drop(columns=["_alias"])

    # Konwersje typów
    df["Clicks"] = pd.to_numeric(df["Clicks"], errors="coerce").fillna(0)
    df["Spend"]  = pd.to_numeric(df["Spend"],  errors="coerce").fillna(0)
    df["DateStart"] = pd.to_datetime(df["DateStart"], errors="coerce").dt.date

    return df

# ─────────────────────────────────────────────
# POBIERANIE – TIKTOK (BQ)
# ─────────────────────────────────────────────
def get_tiktok_data(start_date, end_date):
    """
    Pobiera dane z TikTok w BQ i mapuje na MPK przez advertiser_name.
    Kolumny wynikowe: MPK, advertiser_name, stream_name, date, campaign_id, campaign_name, spend, source
    """
    query = f"""
        SELECT
            advertiser_name,
            stream_name,
            date,
            campaign_id,
            campaign_name,
            spend
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

    # Mapowanie advertiser_name → MPK
    # advertiser_name to alias jak "50PL", "BS" itp. – mapujemy przez TT_Alias lub META_ALIAS_TO_MPK
    df["MPK"] = (
        df["advertiser_name"]
        .astype(str)
        .str.strip()
        .map(tt_alias_to_mpk)          # próba przez secrets TT_Alias
        .fillna(
            df["advertiser_name"].astype(str).str.strip().map(META_ALIAS_TO_MPK)  # fallback
        )
        .fillna("")
    )
    df["source"] = "TikTok"

    df["spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0)
    df["date"]  = pd.to_datetime(df["date"], errors="coerce").dt.date

    return df

# ─────────────────────────────────────────────
# UI – SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.title("📊 GA4 + Social")
    st.markdown("---")

    # ── ŹRÓDŁO DANYCH ─────────────────────────
    st.subheader("📡 Źródło danych")
    data_source = st.radio(
        "Wybierz źródło:",
        ["GA4", "Meta", "TikTok", "Social (Meta + TikTok)", "Wszystko (GA4 + Social)"],
        index=0,
    )
    use_ga4    = data_source in ("GA4", "Wszystko (GA4 + Social)")
    use_meta   = data_source in ("Meta", "Social (Meta + TikTok)", "Wszystko (GA4 + Social)")
    use_tiktok = data_source in ("TikTok", "Social (Meta + TikTok)", "Wszystko (GA4 + Social)")

    st.markdown("---")

    # ── PLATFORMA (tylko dla GA4) ──────────────
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

    # ── WYBÓR SKLEPU ──────────────────────────
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

    # ── ZAKRES DAT ────────────────────────────
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

    # ── PORÓWNANIE ────────────────────────────
    st.markdown("---")
    st.subheader("🔁 Porównanie (opcjonalne)")
    compare_mode = st.selectbox(
        "Porównaj z:",
        ["Brak", "Poprzedni okres", "Rok wcześniej", "Własny zakres"],
    )
    cmp_start = cmp_end = None
    delta = (end_date - start_date).days + 1

    if compare_mode == "Poprzedni okres":
        cmp_end   = start_date - timedelta(days=1)
        cmp_start = cmp_end - timedelta(days=delta - 1)
        st.caption(f"Okres: {cmp_start} → {cmp_end}")
    elif compare_mode == "Rok wcześniej":
        cmp_start = start_date - timedelta(weeks=52)
        cmp_end   = end_date   - timedelta(weeks=52)
        st.caption(f"Okres: {cmp_start} → {cmp_end}")
    elif compare_mode == "Własny zakres":
        cmp_start = st.date_input("Porównaj Od:", value=start_date - timedelta(days=delta))
        cmp_end   = st.date_input("Porównaj Do:", value=start_date - timedelta(days=1))

    st.markdown("---")

    # ── OPCJE EXCEL ───────────────────────────
    st.subheader("💾 Arkusze Excel")
    sheet_per_mpk       = st.checkbox("📂 Zakładka per MPK", value=False)
    sheet_per_mpk_split = st.checkbox(
        "🔀 Porównanie osobno (per MPK)", value=False,
        disabled=(cmp_start is None),
    )

    run_button = st.button("🚀 URUCHOM EKSPORT", use_container_width=True, type="primary")

# ─────────────────────────────────────────────
# GŁÓWNA TREŚĆ
# ─────────────────────────────────────────────
st.title("📊 GA4 + Social Exporter")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Sklepów",   len(filtered_map))
col2.metric("Źródło",    data_source)
col3.metric("Platforma", platform_choice if use_ga4 else "—")
col4.metric("Zakres",    f"{start_date} → {end_date}")

# Poprzedni wynik
if not run_button:
    if "last_results" in st.session_state:
        st.info("Wyniki z poprzedniego eksportu (kliknij 🚀 aby odświeżyć):")
        last = st.session_state["last_results"]
        tabs = st.tabs([k for k in last["dfs"].keys()])
        for tab, (name, df) in zip(tabs, last["dfs"].items()):
            with tab:
                st.dataframe(df, use_container_width=True, height=400)
        if "excel_bytes" in last:
            st.download_button(
                "📥 Pobierz Excel (poprzedni eksport)",
                data=last["excel_bytes"],
                file_name=last["file_name"],
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
    else:
        st.info("Ustaw parametry w panelu bocznym i kliknij **🚀 URUCHOM EKSPORT**.")
    st.stop()

# ─────────────────────────────────────────────
# WALIDACJA
# ─────────────────────────────────────────────
if start_date > end_date:
    st.error("Błędny zakres dat!")
    st.stop()
if filtered_map.empty:
    st.error("Nie wybrano żadnych sklepów!")
    st.stop()

# ─────────────────────────────────────────────
# EKSPORT – GA4
# ─────────────────────────────────────────────
combined_ga4      = pd.DataFrame()
combined_ga4_cmp  = pd.DataFrame()
ga4_per_mpk       = {}
ga4_per_mpk_cmp   = {}

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
            if cmp_start and cmp_end else None
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
        lbl_cmp = (f"{cmp_start.strftime('%d%m%Y')}_{cmp_end.strftime('%d%m%Y')}"
                   if cmp_start else None)

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

# ─────────────────────────────────────────────
# EKSPORT – META
# ─────────────────────────────────────────────
combined_meta     = pd.DataFrame()
combined_meta_cmp = pd.DataFrame()

if use_meta:
    with st.spinner("⏳ Pobieranie danych Meta…"):
        df_meta = get_meta_data(start_date, end_date)
        if not df_meta.empty and selected_mpk_set:
            df_meta = df_meta[df_meta["MPK"].isin(selected_mpk_set)]
        combined_meta = df_meta

        if cmp_start and cmp_end:
            df_meta_cmp = get_meta_data(cmp_start, cmp_end)
            if not df_meta_cmp.empty and selected_mpk_set:
                df_meta_cmp = df_meta_cmp[df_meta_cmp["MPK"].isin(selected_mpk_set)]
            combined_meta_cmp = df_meta_cmp

# ─────────────────────────────────────────────
# EKSPORT – TIKTOK
# ─────────────────────────────────────────────
combined_tiktok     = pd.DataFrame()
combined_tiktok_cmp = pd.DataFrame()

if use_tiktok:
    with st.spinner("⏳ Pobieranie danych TikTok…"):
        df_tt = get_tiktok_data(start_date, end_date)
        if not df_tt.empty and selected_mpk_set:
            df_tt = df_tt[df_tt["MPK"].isin(selected_mpk_set)]
        combined_tiktok = df_tt

        if cmp_start and cmp_end:
            df_tt_cmp = get_tiktok_data(cmp_start, cmp_end)
            if not df_tt_cmp.empty and selected_mpk_set:
                df_tt_cmp = df_tt_cmp[df_tt_cmp["MPK"].isin(selected_mpk_set)]
            combined_tiktok_cmp = df_tt_cmp

# ─────────────────────────────────────────────
# SOCIAL COMBINED (Meta + TikTok) per MPK
# ─────────────────────────────────────────────
def social_summary(meta_df, tiktok_df):
    """Sumuje Spend per MPK z Meta i TikTok."""
    frames = []
    if not meta_df.empty:
        m = meta_df[["MPK", "Spend"]].copy().rename(columns={"Spend": "Spend_Meta"})
        m = m.groupby("MPK", as_index=False)["Spend_Meta"].sum()
        frames.append(m.set_index("MPK"))
    if not tiktok_df.empty:
        t = tiktok_df[["MPK", "spend"]].copy().rename(columns={"spend": "Spend_TikTok"})
        t = t.groupby("MPK", as_index=False)["Spend_TikTok"].sum()
        frames.append(t.set_index("MPK"))
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, axis=1).fillna(0).reset_index()
    result["Spend_Total_Social"] = result.get("Spend_Meta", 0) + result.get("Spend_TikTok", 0)
    return result

social_summary_df     = social_summary(combined_meta, combined_tiktok)
social_summary_cmp_df = social_summary(combined_meta_cmp, combined_tiktok_cmp)

# ─────────────────────────────────────────────
# PODGLĄD
# ─────────────────────────────────────────────
st.success("✅ Dane pobrane!")

tab_labels = []
tab_data   = {}

if use_ga4:
    tab_labels += ["📄 GA4 – Główny", "🔁 GA4 – Porównanie"]
    tab_data["📄 GA4 – Główny"]      = combined_ga4
    tab_data["🔁 GA4 – Porównanie"]  = combined_ga4_cmp

if use_meta:
    tab_labels += ["📘 Meta – Główny", "🔁 Meta – Porównanie"]
    tab_data["📘 Meta – Główny"]     = combined_meta
    tab_data["🔁 Meta – Porównanie"] = combined_meta_cmp

if use_tiktok:
    tab_labels += ["🎵 TikTok – Główny", "🔁 TikTok – Porównanie"]
    tab_data["🎵 TikTok – Główny"]      = combined_tiktok
    tab_data["🔁 TikTok – Porównanie"]  = combined_tiktok_cmp

if use_meta or use_tiktok:
    tab_labels += ["💰 Social Summary"]
    tab_data["💰 Social Summary"] = social_summary_df

tabs = st.tabs(tab_labels)
for tab, name in zip(tabs, tab_labels):
    with tab:
        df_show = tab_data[name]
        if df_show is not None and not df_show.empty:
            st.dataframe(df_show, use_container_width=True, height=400)
            st.caption(f"Wierszy: {len(df_show):,}")
        else:
            st.info("Brak danych.")

# ─────────────────────────────────────────────
# EXCEL
# ─────────────────────────────────────────────
file_name = f"GA4_Social_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
output    = io.BytesIO()

with pd.ExcelWriter(output, engine="openpyxl") as writer:
    sheets_written = 0

    def safe_write(df, sheet_name):
        nonlocal sheets_written
        if df is not None and not df.empty:
            name = sheet_name[:31]
            df.to_excel(writer, sheet_name=name, index=False)
            sheets_written += 1

    # ── Zakładki główne ───────────────────────
    if use_ga4:
        merged_ga4 = pd.concat([combined_ga4, combined_ga4_cmp], ignore_index=True)
        safe_write(merged_ga4,          "GA4_Combined")
        safe_write(combined_ga4,        "GA4_Main")
        if not combined_ga4_cmp.empty:
            safe_write(combined_ga4_cmp, "GA4_Compare")

    if use_meta:
        safe_write(combined_meta,        "Meta_Main")
        if not combined_meta_cmp.empty:
            safe_write(combined_meta_cmp, "Meta_Compare")

    if use_tiktok:
        safe_write(combined_tiktok,      "TikTok_Main")
        if not combined_tiktok_cmp.empty:
            safe_write(combined_tiktok_cmp, "TikTok_Compare")

    if use_meta or use_tiktok:
        safe_write(social_summary_df,    "Social_Summary")
        if not social_summary_cmp_df.empty:
            safe_write(social_summary_cmp_df, "Social_Summary_Cmp")

    # ── Per MPK ───────────────────────────────
    if sheet_per_mpk:
        all_mpks = sorted(selected_mpk_set or set(property_mapping["MPK"]))
        for mpk in all_mpks:
            frames = []
            if use_ga4:
                if mpk in ga4_per_mpk:     frames.append(ga4_per_mpk[mpk].assign(source="GA4_main"))
                if mpk in ga4_per_mpk_cmp: frames.append(ga4_per_mpk_cmp[mpk].assign(source="GA4_cmp"))
            if use_meta and not combined_meta.empty:
                sub = combined_meta[combined_meta["MPK"] == mpk]
                if not sub.empty: frames.append(sub)
            if use_tiktok and not combined_tiktok.empty:
                sub = combined_tiktok[combined_tiktok["MPK"] == mpk]
                if not sub.empty: frames.append(sub)
            if frames:
                safe_write(pd.concat(frames, ignore_index=True), str(mpk))

    # ── Per MPK – porównanie osobno ───────────
    if sheet_per_mpk_split and cmp_start:
        all_mpks = sorted(selected_mpk_set or set(property_mapping["MPK"]))
        for mpk in all_mpks:
            if use_ga4 and mpk in ga4_per_mpk:
                safe_write(ga4_per_mpk[mpk],     f"{mpk}_GA4_m")
            if use_ga4 and mpk in ga4_per_mpk_cmp:
                safe_write(ga4_per_mpk_cmp[mpk], f"{mpk}_GA4_c")
            if use_meta and not combined_meta.empty:
                sub = combined_meta[combined_meta["MPK"] == mpk]
                if not sub.empty: safe_write(sub, f"{mpk}_Meta_m")
            if use_meta and not combined_meta_cmp.empty:
                sub = combined_meta_cmp[combined_meta_cmp["MPK"] == mpk]
                if not sub.empty: safe_write(sub, f"{mpk}_Meta_c")
            if use_tiktok and not combined_tiktok.empty:
                sub = combined_tiktok[combined_tiktok["MPK"] == mpk]
                if not sub.empty: safe_write(sub, f"{mpk}_TT_m")
            if use_tiktok and not combined_tiktok_cmp.empty:
                sub = combined_tiktok_cmp[combined_tiktok_cmp["MPK"] == mpk]
                if not sub.empty: safe_write(sub, f"{mpk}_TT_c")

    if sheets_written == 0:
        pd.DataFrame().to_excel(writer, sheet_name="Empty", index=False)

excel_bytes = output.getvalue()

# Zapisz do session_state
st.session_state["last_results"] = {
    "dfs":        tab_data,
    "excel_bytes": excel_bytes,
    "file_name":   file_name,
}

st.download_button(
    label="📥 Pobierz plik Excel",
    data=excel_bytes,
    file_name=file_name,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
)
