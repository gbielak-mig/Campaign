import streamlit as st
import pandas as pd
import io
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest, FilterExpression,
    Filter, FilterExpressionList
)
from datetime import datetime, timedelta, date

# ─────────────────────────────────────────────
# KONFIGURACJA
# ─────────────────────────────────────────────
st.set_page_config(page_title="GA4 Exporter", layout="wide", page_icon="📊")

# Stałe wymiary i metryki
DIMENSIONS = [
    "sessionSource",
    "sessionMedium",
    "sessionCampaignName",
]

METRICS = [
    "sessions",
    "transactions",
    "totalRevenue",
    "advertiserAdCost",
]

# Mapowanie platformy na wartości operatingSystemWithVersion / platform
# GA4 używa wymiaru "platform" (WEB, IOS, ANDROID)
PLATFORM_FILTERS = {
    "Web":  ["WEB"],
    "App":  ["IOS", "ANDROID"],
}

# ─────────────────────────────────────────────
# KROK 1 – HASŁO
# ─────────────────────────────────────────────
if not st.session_state.get("authenticated"):
    st.title("🔐 GA4 Exporter")
    pwd = st.text_input("Hasło dostępu:", type="password")
    if st.button("Zaloguj", use_container_width=True):
        if pwd == st.secrets["app"]["password"]:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("❌ Błędne hasło!")
    st.stop()

# ─────────────────────────────────────────────
# KROK 2 – GA4 CLIENT
# ─────────────────────────────────────────────
@st.cache_resource
def get_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    return BetaAnalyticsDataClient(credentials=credentials)

try:
    client = get_client()
except Exception as e:
    st.error(f"Błąd połączenia z Google Analytics: {e}")
    st.stop()

# ─────────────────────────────────────────────
# MAPOWANIE SKLEPÓW
# ─────────────────────────────────────────────
property_mapping = pd.DataFrame([
    {"MPK": mpk, "ID_GA4": int(vals[0]), "Brand": vals[1]}
    for mpk, vals in st.secrets["ga4_properties"].items()
])

# ─────────────────────────────────────────────
# POBIERANIE DANYCH
# ─────────────────────────────────────────────
def build_platform_filter(platform_values: list[str]) -> FilterExpression | None:
    """Buduje filtr GA4 dla wybranej platformy (WEB / IOS+ANDROID)."""
    if not platform_values:
        return None

    expressions = [
        FilterExpression(
            filter=Filter(
                field_name="platform",
                string_filter=Filter.StringFilter(
                    match_type=Filter.StringFilter.MatchType.EXACT,
                    value=pv,
                    case_sensitive=False,
                ),
            )
        )
        for pv in platform_values
    ]

    if len(expressions) == 1:
        return expressions[0]

    return FilterExpression(
        or_group=FilterExpressionList(expressions=expressions)
    )


def get_ga4_data(property_id: int, start_date, end_date,
                 platform_filter_expr=None) -> pd.DataFrame:
    """Pobiera dane GA4 dla danego property i zakresu dat."""
    try:
        request = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name=d) for d in DIMENSIONS],
            metrics=[Metric(name=m) for m in METRICS],
            date_ranges=[DateRange(start_date=str(start_date), end_date=str(end_date))],
            dimension_filter=platform_filter_expr,
        )
        response = client.run_report(request)

        rows = []
        for row in response.rows:
            rd = {DIMENSIONS[i]: v.value for i, v in enumerate(row.dimension_values)}
            for i, mv in enumerate(row.metric_values):
                rd[METRICS[i]] = mv.value
            rows.append(rd)

        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=DIMENSIONS + METRICS)
        for m in METRICS:
            if m in df.columns:
                df[m] = pd.to_numeric(df[m], errors="coerce").fillna(0)
        return df

    except Exception as e:
        st.warning(f"Błąd dla ID {property_id}: {e}")
        return pd.DataFrame(columns=DIMENSIONS + METRICS)

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

# ─────────────────────────────────────────────
# UI – SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.title("📊 GA4 Exporter")
    st.markdown("---")

    # ── WYBÓR SKLEPU ──────────────────────────
    st.subheader("🏬 Sklep")
    brand_options = sorted(property_mapping["Brand"].unique().tolist())
    selected_brands = st.multiselect("Brand (brak = wszystkie):", options=brand_options, default=[])

    if selected_brands:
        filtered_map = property_mapping[property_mapping["Brand"].isin(selected_brands)]
    else:
        filtered_map = property_mapping.copy()

    mpk_options = sorted(filtered_map["MPK"].tolist())
    selected_mpks = st.multiselect("MPK (brak = wszystkie):", options=mpk_options, default=[])

    if selected_mpks:
        filtered_map = filtered_map[filtered_map["MPK"].isin(selected_mpks)]

    st.caption(f"Wybrano {len(filtered_map)} sklep(ów)")
    st.markdown("---")

    # ── PLATFORMA ─────────────────────────────
    st.subheader("📱 Platforma")
    platform_choice = st.radio(
        "Źródło danych:",
        options=["Web", "App", "Web + App"],
        index=2,
        help="App = iOS + Android łącznie"
    )
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
        ["Brak", "Poprzedni okres", "Rok wcześniej", "Własny zakres"]
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
        disabled=(cmp_start is None)
    )

    run_button = st.button("🚀 URUCHOM EKSPORT", use_container_width=True, type="primary")

# ─────────────────────────────────────────────
# GŁÓWNA TREŚĆ
# ─────────────────────────────────────────────
st.title("📊 GA4 Exporter")

# Podsumowanie wybranych parametrów
col1, col2, col3 = st.columns(3)
col1.metric("Sklepów", len(filtered_map))
col2.metric("Platforma", platform_choice)
col3.metric("Zakres", f"{start_date} → {end_date}")

st.markdown("**Pobierane pola:**")
st.caption(
    f"Wymiary: {', '.join(DIMENSIONS)}  |  "
    f"Metryki: {', '.join(METRICS)}"
)

# ─── Poprzedni wynik (bez ponownego uruchamiania) ───
if not run_button:
    if "last_combined_df" in st.session_state:
        st.info("Wyniki z poprzedniego eksportu (kliknij 🚀 aby odświeżyć):")
        _tab_m, _tab_c = st.tabs(["📄 Dane główne", "🔁 Dane porównawcze"])
        with _tab_m:
            st.dataframe(st.session_state["last_combined_df"], use_container_width=True, height=400)
        with _tab_c:
            cmp_df = st.session_state.get("last_combined_cmp", pd.DataFrame())
            if not cmp_df.empty:
                st.dataframe(cmp_df, use_container_width=True, height=400)
            else:
                st.info("Brak danych porównawczych.")
        if "last_excel_bytes" in st.session_state:
            st.download_button(
                label="📥 Pobierz Excel (poprzedni eksport)",
                data=st.session_state["last_excel_bytes"],
                file_name=st.session_state.get("last_file_name", "GA4_Export.xlsx"),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
    else:
        st.info("Ustaw parametry w panelu bocznym i kliknij **🚀 URUCHOM EKSPORT**.")
    st.stop()

# ─────────────────────────────────────────────
# EKSPORT
# ─────────────────────────────────────────────
if start_date > end_date:
    st.error("Błędny zakres dat!")
    st.stop()

if filtered_map.empty:
    st.error("Nie wybrano żadnych sklepów!")
    st.stop()

# Ustal filtry platformy
if platform_choice == "Web + App":
    platform_expr = None  # brak filtra = wszystkie platformy
elif platform_choice == "Web":
    platform_expr = build_platform_filter(PLATFORM_FILTERS["Web"])
else:  # App
    platform_expr = build_platform_filter(PLATFORM_FILTERS["App"])

all_results     = {}
all_results_cmp = {}
combined_df     = pd.DataFrame()
combined_cmp    = pd.DataFrame()

progress_bar = st.progress(0)
status_text  = st.empty()

for i, (_, row) in enumerate(filtered_map.iterrows()):
    status_text.write(f"⏳ Pobieranie: **{row['MPK']}** ({row['Brand']})...")

    df_main = get_ga4_data(row["ID_GA4"], start_date, end_date, platform_expr)
    df_cmp  = (
        get_ga4_data(row["ID_GA4"], cmp_start, cmp_end, platform_expr)
        if cmp_start and cmp_end else None
    )

    def enrich(df, label):
        if df is not None and not df.empty:
            df = df.copy()
            df["MPK"]        = row["MPK"]
            df["Brand"]      = row["Brand"]
            df["Platforma"]  = platform_choice
            df["date_range"] = label
        return df

    date_label     = f"{start_date.strftime('%d%m%Y')}_{end_date.strftime('%d%m%Y')}"
    date_label_cmp = (
        f"{cmp_start.strftime('%d%m%Y')}_{cmp_end.strftime('%d%m%Y')}"
        if cmp_start else None
    )

    df_main = enrich(df_main, date_label)
    df_cmp  = enrich(df_cmp,  date_label_cmp)

    if df_main is not None and not df_main.empty:
        all_results[row["MPK"]] = df_main
        combined_df = pd.concat([combined_df, df_main], ignore_index=True)

    if df_cmp is not None and not df_cmp.empty:
        all_results_cmp[row["MPK"]] = df_cmp
        combined_cmp = pd.concat([combined_cmp, df_cmp], ignore_index=True)

    progress_bar.progress((i + 1) / len(filtered_map))

status_text.empty()
progress_bar.empty()

# ─────────────────────────────────────────────
# PODGLĄD
# ─────────────────────────────────────────────
st.success(f"✅ Pobrano dane dla {len(all_results)} sklepów.")

tab_main, tab_cmp = st.tabs(["📄 Dane główne", "🔁 Dane porównawcze"])
with tab_main:
    st.dataframe(combined_df, use_container_width=True, height=400)
    st.caption(f"Łącznie wierszy: {len(combined_df):,}")
with tab_cmp:
    if not combined_cmp.empty:
        st.dataframe(combined_cmp, use_container_width=True, height=400)
        st.caption(f"Łącznie wierszy (porównanie): {len(combined_cmp):,}")
    else:
        st.info("Brak danych porównawczych.")

st.session_state["last_combined_df"]  = combined_df
st.session_state["last_combined_cmp"] = combined_cmp

# ─────────────────────────────────────────────
# EXCEL
# ─────────────────────────────────────────────
file_name = f"GA4_Export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
output    = io.BytesIO()

with pd.ExcelWriter(output, engine="openpyxl") as writer:
    sheets_written = 0

    # Domyślnie: Combined_All
    merged_all = pd.concat([combined_df, combined_cmp], ignore_index=True)
    if not merged_all.empty:
        merged_all.to_excel(writer, sheet_name="Combined_All", index=False)
        sheets_written += 1

    # Per MPK (razem)
    if sheet_per_mpk:
        all_mpks = sorted(set(list(all_results) + list(all_results_cmp)))
        for mpk in all_mpks:
            frames = []
            if mpk in all_results:     frames.append(all_results[mpk])
            if mpk in all_results_cmp: frames.append(all_results_cmp[mpk])
            if frames:
                pd.concat(frames, ignore_index=True).to_excel(
                    writer, sheet_name=str(mpk)[:31], index=False
                )
                sheets_written += 1

    # Per MPK – porównanie osobno
    if sheet_per_mpk_split and cmp_start:
        all_mpks = sorted(set(list(all_results) + list(all_results_cmp)))
        for mpk in all_mpks:
            if mpk in all_results:
                all_results[mpk].to_excel(writer, sheet_name=f"{mpk}_main"[:31], index=False)
                sheets_written += 1
            if mpk in all_results_cmp:
                all_results_cmp[mpk].to_excel(writer, sheet_name=f"{mpk}_cmp"[:31], index=False)
                sheets_written += 1

    if sheets_written == 0:
        pd.DataFrame().to_excel(writer, sheet_name="Empty", index=False)

excel_bytes = output.getvalue()
st.session_state["last_excel_bytes"] = excel_bytes
st.session_state["last_file_name"]   = file_name

st.download_button(
    label="📥 Pobierz plik Excel",
    data=excel_bytes,
    file_name=file_name,
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True,
)