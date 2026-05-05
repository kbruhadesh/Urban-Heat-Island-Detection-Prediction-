"""
Phase 5: Streamlit Dashboard for Urban Heat Island Analysis
Premium dark-themed dashboard with 7 interactive tabs.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import st_folium
import os
import numpy as np

# ── Page Config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="🌡️ Urban Heat Island Dashboard",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Load Custom CSS ────────────────────────────────────────────────────────
css_path = os.path.join(os.path.dirname(__file__), "styles.css")
if os.path.exists(css_path):
    with open(css_path) as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# ── City Coordinates (for map) ─────────────────────────────────────────────
CITY_COORDS = {
    "Delhi": [28.64, 77.10], "Mumbai": [19.08, 72.88],
    "Bangalore": [12.98, 77.60], "Chennai": [13.08, 80.24],
    "Hyderabad": [17.40, 78.47], "Kochi": [9.97, 76.30],
    "Pune": [18.58, 73.87], "Ahmedabad": [23.02, 72.60],
    "Kolkata": [22.60, 88.35], "Jaipur": [26.90, 75.80],
    "Surat": [21.20, 72.83]
}

SEASON_ORDER = ["Pre-Monsoon", "Monsoon", "Post-Monsoon", "Winter"]

# ── Plotly Theme ───────────────────────────────────────────────────────────
PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color="#f3f4f6"),
    margin=dict(l=40, r=20, t=50, b=40),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor="rgba(75,85,99,0.3)", borderwidth=1),
)

FIRE_COLORS = ["#f97316", "#ef4444", "#dc2626", "#f59e0b", "#eab308",
               "#10b981", "#3b82f6", "#8b5cf6", "#ec4899", "#06b6d4", "#84cc16"]


# ── Data Loading (cached) ─────────────────────────────────────────────────
@st.cache_data
def load_data():
    base = os.path.join(os.path.dirname(__file__), "..", "data", "output")
    data = {}
    files = {
        "all_clean": "uhi_all_clean.csv",
        "monthly": "uhi_monthly.csv",
        "yearly": "uhi_yearly.csv",
        "seasonal": "uhi_seasonal.csv",
        "decade": "uhi_decade.csv",
        "rankings": "city_rankings.csv",
        "rate_of_change": "uhi_rate_of_change.csv",
        "hotspots": "hotspot_alerts.csv",
        "extreme_heat": "extreme_heat_events.csv",
        "peak_months": "peak_months.csv",
        "moving_avg": "uhi_moving_avg.csv",
        "forecast": "uhi_forecast_2025_2030.csv",
        "test_predictions": "test_predictions.csv",
        "seasonal_evolution": "seasonal_evolution.csv",
        "yoy_trends": "uhi_yoy_trends.csv",
    }
    for key, fname in files.items():
        path = os.path.join(base, fname)
        if os.path.exists(path):
            data[key] = pd.read_csv(path)
        else:
            data[key] = pd.DataFrame()
    return data


data = load_data()

# ── Sidebar ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🌡️ UHI Dashboard")
    st.markdown("---")

    cities = sorted(data["rankings"]["city"].unique()) if not data["rankings"].empty else []
    selected_cities = st.multiselect("🏙️ Select Cities", cities, default=cities[:5] if len(cities) >= 5 else cities)

    if not data["yearly"].empty:
        years = sorted(data["yearly"]["year"].unique())
        year_range = st.slider("📅 Year Range", int(min(years)), int(max(years)),
                               (int(min(years)), int(max(years))))
    else:
        year_range = (2000, 2024)

    st.markdown("---")
    st.markdown("### 📊 Data Summary")
    if not data["all_clean"].empty:
        st.metric("Total Records", f"{len(data['all_clean']):,}")
        st.metric("Cities", len(cities))
        st.metric("Year Span", f"{year_range[0]}–{year_range[1]}")

    st.markdown("---")
    st.caption("📡 Data: MODIS MOD11A2 (Terra)")
    st.caption("🔧 Processing: Apache Spark")
    st.caption("🤖 ML: Spark MLlib GBTRegressor")


# ── Filter helper ──────────────────────────────────────────────────────────
def filter_df(df, city_col="city", year_col="year"):
    if df.empty:
        return df
    filtered = df.copy()
    if selected_cities and city_col in filtered.columns:
        filtered = filtered[filtered[city_col].isin(selected_cities)]
    if year_col in filtered.columns:
        try:
            year_series = pd.to_numeric(filtered[year_col], errors="coerce")
            mask = (year_series >= year_range[0]) & (year_series <= year_range[1])
            filtered = filtered[mask]
        except Exception:
            pass  # skip year filter if column can't be compared
    return filtered


# ══════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════
tabs = st.tabs([
    "🏠 Overview", "📈 Trends", "🗺️ Heatmap", "🏙️ City Comparison",
    "🔮 Forecast", "⚠️ Alerts", "📋 Policy"
])

# ── TAB 1: Overview ───────────────────────────────────────────────────────
with tabs[0]:
    st.markdown("# 🌡️ Urban Heat Island Dashboard")
    st.markdown("*Analyzing UHI patterns across 11 Indian cities (2000–2024) using MODIS satellite data & Apache Spark*")

    if not data["rankings"].empty:
        rankings = data["rankings"].sort_values("avg_uhi", ascending=False)

        col1, col2, col3, col4 = st.columns(4)
        hottest = rankings.iloc[0]
        coolest = rankings.iloc[-1]

        col1.metric("🔥 Highest UHI City", hottest["city"], f"{hottest['avg_uhi']:.2f}°C")
        col2.metric("❄️ Lowest UHI City", coolest["city"], f"{coolest['avg_uhi']:.2f}°C")
        col3.metric("🌡️ Avg Urban Temp", f"{rankings['avg_urban_temp'].mean():.1f}°C")
        col4.metric("🌙 Avg Night Temp", f"{rankings['avg_night_temp'].mean():.1f}°C")

        st.markdown("---")
        st.markdown("## 🏆 City UHI Rankings")

        fig = px.bar(rankings, x="city", y="avg_uhi",
                     color="avg_uhi", color_continuous_scale=["#3b82f6", "#f97316", "#ef4444"],
                     labels={"avg_uhi": "Avg UHI (°C)", "city": "City"})
        fig.update_layout(**PLOTLY_LAYOUT, title="Average UHI Index by City (2000-2024)",
                          coloraxis_colorbar_title="UHI °C", height=450)
        st.plotly_chart(fig, use_container_width=True)

    if not data["rate_of_change"].empty:
        st.markdown("## 📊 Rate of UHI Change (°C per decade)")
        roc = data["rate_of_change"].sort_values("rate_per_decade_c", ascending=False)
        fig2 = px.bar(roc, x="city", y="rate_per_decade_c",
                      color="trend", color_discrete_map={"Warming": "#ef4444", "Cooling": "#3b82f6", "Stable": "#6b7280"},
                      labels={"rate_per_decade_c": "Rate (°C/decade)"})
        fig2.update_layout(**PLOTLY_LAYOUT, title="UHI Trend Direction per City", height=400)
        st.plotly_chart(fig2, use_container_width=True)

# ── TAB 2: Trends ─────────────────────────────────────────────────────────
with tabs[1]:
    st.markdown("# 📈 UHI Trend Analysis")

    if not data["yearly"].empty:
        yearly_f = filter_df(data["yearly"])

        st.markdown("## Yearly Average UHI")
        fig = px.line(yearly_f, x="year", y="avg_uhi", color="city",
                      color_discrete_sequence=FIRE_COLORS,
                      labels={"avg_uhi": "Avg UHI (°C)", "year": "Year"})
        fig.update_layout(**PLOTLY_LAYOUT, title="Yearly UHI Index Trends", height=500)
        fig.update_traces(line=dict(width=2))
        st.plotly_chart(fig, use_container_width=True)

    if not data["moving_avg"].empty:
        st.markdown("## 5-Year Moving Average")
        ma_f = filter_df(data["moving_avg"])
        fig2 = px.line(ma_f, x="year", y="moving_avg_5yr", color="city",
                       color_discrete_sequence=FIRE_COLORS,
                       labels={"moving_avg_5yr": "5yr Moving Avg UHI (°C)"})
        fig2.update_layout(**PLOTLY_LAYOUT, title="Smoothed UHI Trends (5-Year Moving Average)", height=450)
        st.plotly_chart(fig2, use_container_width=True)

    if not data["decade"].empty:
        st.markdown("## Decade Comparison")
        decade_f = data["decade"]
        if selected_cities:
            decade_f = decade_f[decade_f["city"].isin(selected_cities)]

        fig3 = px.bar(decade_f, x="city", y="avg_uhi", color="decade", barmode="group",
                      color_discrete_sequence=["#3b82f6", "#f97316", "#ef4444"],
                      labels={"avg_uhi": "Avg UHI (°C)"})
        fig3.update_layout(**PLOTLY_LAYOUT, title="UHI by Decade", height=450)
        st.plotly_chart(fig3, use_container_width=True)

# ── TAB 3: Heatmap ────────────────────────────────────────────────────────
with tabs[2]:
    st.markdown("# 🗺️ UHI Intensity Map")

    if not data["rankings"].empty:
        rankings = data["rankings"]
        if selected_cities:
            rankings = rankings[rankings["city"].isin(selected_cities)]

        m = folium.Map(location=[22.5, 78.5], zoom_start=5,
                       tiles="CartoDB dark_matter")

        for _, row in rankings.iterrows():
            city = row["city"]
            if city in CITY_COORDS:
                coords = CITY_COORDS[city]
                uhi = row["avg_uhi"]

                color = "red" if uhi > 0.3 else ("orange" if uhi > 0 else "blue")
                radius = max(abs(uhi) * 15, 5)

                folium.CircleMarker(
                    location=coords, radius=radius,
                    color=color, fill=True, fill_opacity=0.7,
                    popup=folium.Popup(
                        f"<b>{city}</b><br>UHI: {uhi:.2f}°C<br>"
                        f"Urban: {row['avg_urban_temp']:.1f}°C<br>"
                        f"Peak: {row['peak_uhi']:.2f}°C",
                        max_width=200
                    ),
                    tooltip=f"{city}: {uhi:.2f}°C"
                ).add_to(m)

        st_folium(m, width=None, height=550)

        st.markdown("### 🌡️ Monthly UHI Heatmap")
        if not data["monthly"].empty:
            monthly_f = filter_df(data["monthly"])
            if not monthly_f.empty:
                pivot = monthly_f.groupby(["city", "month"])["avg_uhi"].mean().reset_index()
                pivot_table = pivot.pivot(index="city", columns="month", values="avg_uhi")
                month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                pivot_table.columns = month_names[:len(pivot_table.columns)]

                fig = px.imshow(pivot_table, color_continuous_scale="RdYlBu_r",
                                labels=dict(x="Month", y="City", color="UHI °C"),
                                aspect="auto")
                fig.update_layout(**PLOTLY_LAYOUT, title="Monthly UHI Intensity Heatmap", height=450)
                st.plotly_chart(fig, use_container_width=True)

# ── TAB 4: City Comparison ────────────────────────────────────────────────
with tabs[3]:
    st.markdown("# 🏙️ City Comparison")

    if not data["seasonal"].empty and selected_cities:
        st.markdown("## Seasonal UHI Patterns")
        seasonal_f = data["seasonal"][data["seasonal"]["city"].isin(selected_cities)]

        fig = px.bar(seasonal_f, x="season", y="avg_uhi", color="city",
                     barmode="group", color_discrete_sequence=FIRE_COLORS,
                     category_orders={"season": SEASON_ORDER},
                     labels={"avg_uhi": "Avg UHI (°C)"})
        fig.update_layout(**PLOTLY_LAYOUT, title="Seasonal UHI Comparison", height=450)
        st.plotly_chart(fig, use_container_width=True)

    if not data["rankings"].empty and selected_cities:
        st.markdown("## Temperature Comparison")
        rank_f = data["rankings"][data["rankings"]["city"].isin(selected_cities)]

        fig2 = make_subplots(rows=1, cols=2,
                             subplot_titles=("Urban Day Temperature", "Night Temperature"))
        fig2.add_trace(go.Bar(x=rank_f["city"], y=rank_f["avg_urban_temp"],
                              marker_color="#f97316", name="Day"), row=1, col=1)
        fig2.add_trace(go.Bar(x=rank_f["city"], y=rank_f["avg_night_temp"],
                              marker_color="#3b82f6", name="Night"), row=1, col=2)
        fig2.update_layout(**PLOTLY_LAYOUT, height=400, showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    if not data["peak_months"].empty:
        st.markdown("## Peak UHI Month per City")
        peak_f = data["peak_months"]
        if selected_cities:
            peak_f = peak_f[peak_f["city"].isin(selected_cities)]
        month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                       7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
        peak_f = peak_f.copy()
        peak_f["peak_month_name"] = peak_f["peak_month"].map(month_names)
        st.dataframe(peak_f[["city", "peak_month_name", "peak_uhi"]].rename(
            columns={"peak_month_name": "Peak Month", "peak_uhi": "Peak UHI (°C)"}
        ), use_container_width=True, hide_index=True)

# ── TAB 5: Forecast ───────────────────────────────────────────────────────

with tabs[4]:
    st.markdown("# 🔮 UHI Forecast (2025–2030)")

    if not data["forecast"].empty and not data["yearly"].empty:
        st.markdown("## Historical + Predicted UHI")

        for city in (selected_cities or cities[:3]):

            # =========================
            # HISTORICAL (FILTERED CORRECTLY)
            # =========================
            hist = data["yearly"][data["yearly"]["city"] == city][["year", "avg_uhi"]].copy()
            hist = hist.rename(columns={"avg_uhi": "uhi"})

            # 🔥 IMPORTANT FIX
            hist["year"] = pd.to_numeric(hist["year"], errors="coerce")
            hist = hist[
                (hist["year"] >= year_range[0]) &
                (hist["year"] <= year_range[1])
            ]

            hist["type"] = "Historical"

            # =========================
            # FORECAST (DO NOT FILTER)
            # =========================
            fore = data["forecast"][data["forecast"]["city"] == city].copy()

            if not fore.empty:

                # 🔥 ENSURE TYPES
                fore["year"] = pd.to_numeric(fore["year"], errors="coerce")
                fore["predicted_uhi"] = pd.to_numeric(fore["predicted_uhi"], errors="coerce")

                # 🔥 CORRECT AGGREGATION
                fore_yearly = (
                    fore.groupby("year", as_index=False)["predicted_uhi"]
                    .mean()
                    .rename(columns={"predicted_uhi": "uhi"})
                )

                fore_yearly["type"] = "Forecast"

                # =========================
                # COMBINE SAFELY
                # =========================
                combined = pd.concat([hist, fore_yearly], ignore_index=True)

            else:
                combined = hist.copy()

            # 🔥 FINAL FIX (SORT + CLEAN)
            combined["year"] = pd.to_numeric(combined["year"], errors="coerce")
            combined = combined.dropna(subset=["year", "uhi"])
            combined = combined.sort_values(by="year").reset_index(drop=True)

            # =========================
            # PLOT (UNCHANGED)
            # =========================
            fig = px.line(
                combined,
                x="year",
                y="uhi",
                color="type",
                color_discrete_map={
                    "Historical": "#f97316",
                    "Forecast": "#8b5cf6"
                },
                labels={"uhi": "UHI Index (°C)", "year": "Year"}
            )

            fig.update_layout(
                **PLOTLY_LAYOUT,
                title=f"{city} — Historical vs Forecast UHI",
                height=350
            )

            fig.update_traces(line=dict(width=3))

            # Forecast dashed
            if len(fig.data) > 1:
                fig.data[1].update(line=dict(dash="dash"))

            st.plotly_chart(fig, use_container_width=True)
            
# ── TAB 6: Alerts ─────────────────────────────────────────────────────────
with tabs[5]:
    st.markdown("# ⚠️ Heat Alerts & Hotspots")

    if not data["hotspots"].empty:
        hotspots_f = filter_df(data["hotspots"])

        col1, col2, col3 = st.columns(3)
        critical = hotspots_f[hotspots_f["severity"] == "CRITICAL"] if "severity" in hotspots_f.columns else pd.DataFrame()
        high = hotspots_f[hotspots_f["severity"] == "HIGH"] if "severity" in hotspots_f.columns else pd.DataFrame()

        col1.metric("🔴 Critical Events", len(critical))
        col2.metric("🟠 High Events", len(high))
        col3.metric("📊 Total Hotspots", len(hotspots_f))

        st.markdown("### Hotspot Events")
        st.dataframe(hotspots_f.head(100), use_container_width=True, hide_index=True)

    if not data["extreme_heat"].empty:
        st.markdown("### 🌡️ Extreme Heat Events (Urban > 45°C)")
        extreme_f = filter_df(data["extreme_heat"])
        st.dataframe(extreme_f.head(50), use_container_width=True, hide_index=True)

# ── TAB 7: Policy ─────────────────────────────────────────────────────────
with tabs[6]:
    st.markdown("# 📋 Policy Recommendations")

    if not data["rankings"].empty:
        for _, row in data["rankings"].iterrows():
            city = row["city"]
            uhi = row["avg_uhi"]
            peak = row["peak_uhi"]

            if city not in (selected_cities or cities):
                continue

            with st.expander(f"🏙️ {city} — UHI: {uhi:.2f}°C", expanded=(uhi > 0.3)):
                if uhi > 0.3:
                    st.error(f"**HIGH PRIORITY**: {city} shows significant urban heating (+{uhi:.2f}°C)")
                    st.markdown("""
                    **Recommended Actions:**
                    - 🌳 Increase urban green cover by 15-20% in core areas
                    - 🏗️ Mandate cool roof coatings on new constructions
                    - 💧 Implement permeable pavements in commercial zones
                    - 🌊 Develop urban water bodies / mist cooling systems
                    """)
                elif uhi > 0:
                    st.warning(f"**MODERATE**: {city} has mild urban heating (+{uhi:.2f}°C)")
                    st.markdown("""
                    **Recommended Actions:**
                    - 🌳 Maintain existing green cover and expand parks
                    - 🏗️ Encourage green building certifications
                    - 📊 Continue monitoring with annual assessments
                    """)
                else:
                    st.success(f"**LOW RISK**: {city} is cooler than surroundings ({uhi:.2f}°C)")
                    st.markdown("""
                    **Note:** Negative UHI typically indicates coastal/green influence.
                    - ✅ Preserve existing green and blue infrastructure
                    - 📊 Monitor for future changes as city expands
                    """)

                if peak > 5:
                    st.warning(f"⚠️ Peak UHI reached {peak:.2f}°C — extreme event preparedness recommended")

# ── Footer ─────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div class='footer'>"
    "🌡️ Urban Heat Island Detection & Prediction System | "
    "Data: MODIS MOD11A2 (2000–2024) | "
    "Processing: Apache Spark + Kafka | "
    "ML: Spark MLlib GBTRegressor"
    "</div>",
    unsafe_allow_html=True
)
