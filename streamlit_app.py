import streamlit as st
import pandas as pd
import psycopg2
import pydeck as pdk
import plotly.express as px
import numpy as np

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Beaver Watershed Pipeline",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Dark theme CSS + Consolas font + teal multiselect tags ────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Source+Code+Pro:wght@300;400;600;700&display=swap');

  html, body, [class*="css"], .stApp {
      font-family: 'Source Code Pro', Consolas, 'Courier New', monospace !important;
      background-color: #0d0d0d !important;
      color: #e0e0e0 !important;
  }

  section[data-testid="stSidebar"] {
      background-color: #111111 !important;
      border-right: 1px solid #222 !important;
  }
  section[data-testid="stSidebar"] * {
      color: #cccccc !important;
  }

  .block-container {
      padding-top: 1.5rem;
      padding-bottom: 1rem;
      background-color: #0d0d0d !important;
  }

  h1, h2, h3, h4 {
      font-family: 'Source Code Pro', Consolas, monospace !important;
      color: #00ff9f !important;
      font-weight: 700 !important;
      letter-spacing: -0.5px;
  }

  p, li, label, .stMarkdown {
      font-family: 'Source Code Pro', Consolas, monospace !important;
      color: #cccccc !important;
  }

  [data-testid="metric-container"] {
      background-color: #161616 !important;
      border: 1px solid #2a2a2a !important;
      border-radius: 6px !important;
      padding: 12px 16px !important;
  }
  [data-testid="metric-container"] label {
      color: #888888 !important;
      font-size: 0.72rem !important;
      text-transform: uppercase;
      letter-spacing: 0.05em;
  }
  [data-testid="metric-container"] [data-testid="stMetricValue"] {
      color: #00ff9f !important;
      font-size: 1.4rem !important;
      font-weight: 700 !important;
  }

  span[data-baseweb="tag"] {
      background-color: #003d2e !important;
      border: 1px solid #00ff9f !important;
      border-radius: 4px !important;
  }
  span[data-baseweb="tag"] span {
      color: #00ff9f !important;
  }

  div[data-baseweb="select"] > div {
      background-color: #161616 !important;
      border-color: #2a2a2a !important;
  }

  [data-testid="stSlider"] * { color: #cccccc !important; }

  hr { border-color: #222222 !important; }

  details {
      background-color: #111111 !important;
      border: 1px solid #222 !important;
      border-radius: 6px !important;
  }
  details summary { color: #cccccc !important; }

  [data-testid="stInfo"] {
      background-color: #0a1f18 !important;
      border-left: 3px solid #00ff9f !important;
      color: #cccccc !important;
  }

  table { background-color: #111111 !important; color: #cccccc !important; }
  th { color: #00ff9f !important; border-bottom: 1px solid #2a2a2a !important; }
  td { border-bottom: 1px solid #1a1a1a !important; }

[data-testid="stDataFrame"] { background-color: #111111 !important; }
  header[data-testid="stHeader"] { visibility: hidden !important; }
  #MainMenu { visibility: hidden !important; }
  footer { visibility: hidden !important; }
</style>
""", unsafe_allow_html=True)

# ── Dark plotly template ──────────────────────────────────────────────────────
DARK_BG  = "#0d0d0d"
PLOT_BG  = "#111111"
GRID_CLR = "#222222"
TEXT_CLR = "#cccccc"
NEON     = "#00ff9f"

def dark_layout(fig, height=400):
    fig.update_layout(
        height=height,
        margin=dict(l=0, r=10, t=10, b=10),
        paper_bgcolor=DARK_BG,
        plot_bgcolor=PLOT_BG,
        font=dict(family="'Source Code Pro', Consolas, monospace", color=TEXT_CLR, size=11),
        xaxis=dict(gridcolor=GRID_CLR, zerolinecolor=GRID_CLR, color=TEXT_CLR),
        yaxis=dict(gridcolor=GRID_CLR, zerolinecolor=GRID_CLR, color=TEXT_CLR),
        legend=dict(bgcolor="#111111", bordercolor="#2a2a2a", borderwidth=1, font=dict(color=TEXT_CLR)),
    )
    return fig


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=86400)
def load_data():
    try:
        conn = psycopg2.connect(
            host=st.secrets["postgres"]["host"],
            port=st.secrets["postgres"]["port"],
            dbname=st.secrets["postgres"]["dbname"],
            user=st.secrets["postgres"]["user"],
            password=st.secrets["postgres"]["password"],
        )
        df = pd.read_sql("SELECT * FROM beaver_water_joined", conn)
        conn.close()
        df = df[df["avg_dissolved_oxygen"] > 0]
        df["year"] = df["year"].astype("Int64")
        return df
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        st.stop()


# ── Sidebar ───────────────────────────────────────────────────────────────────
def render_sidebar(df):
    st.sidebar.header("// Filters")

    states = sorted(df["state_province"].dropna().unique().tolist())
    selected_states = st.sidebar.multiselect("State", options=states, default=states)

    do_min = float(df["avg_dissolved_oxygen"].min())
    do_max = float(df["avg_dissolved_oxygen"].max())
    do_range = st.sidebar.slider(
        "Dissolved Oxygen (mg/L)",
        min_value=round(do_min, 1), max_value=round(do_max, 1),
        value=(round(do_min, 1), round(do_max, 1)), step=0.1,
    )

    dist_max = float(df["distance_km"].max())
    max_distance = st.sidebar.slider(
        "Max Distance to Station (km)",
        min_value=0, max_value=int(dist_max), value=int(dist_max), step=10,
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "End-to-end AWS data pipeline ingesting GBIF beaver occurrence "
        "records and USGS dissolved oxygen readings, spatially joined and visualized here."
    )
    st.sidebar.markdown("[View on GitHub](https://github.com/yvnnhong/beaver-watershed-pipeline)")

    return selected_states, do_range, max_distance


# ── DO color scale ────────────────────────────────────────────────────────────
def do_to_rgb(value, vmin=6.0, vmax=12.0):
    t = max(0.0, min(1.0, (value - vmin) / (vmax - vmin)))
    r = int(230 * (1 - t))
    g = int(180 + 75 * t)
    b = int(80 * (1 - t))
    return [r, g, b, 210]


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    st.title("Beaver Watershed Pipeline")
    st.markdown(
        "Weekly-automated AWS data pipeline spatially joining **GBIF** beaver sighting records "
        "with **USGS Water Services** water quality data (dissolved oxygen, temperature, pH, turbidity) "
        "to identify anomalous monitoring stations near beaver habitat.  "
        "Pipeline: **AWS Lambda × 4 → Step Functions → S3 → RDS PostgreSQL → Isolation Forest anomaly detection**."
    )
    st.markdown("---")

    with st.spinner("Loading data from RDS..."):
        df_raw = load_data()

    selected_states, do_range, max_distance = render_sidebar(df_raw)

    df = df_raw[
        (df_raw["state_province"].isin(selected_states))
        & (df_raw["avg_dissolved_oxygen"] >= do_range[0])
        & (df_raw["avg_dissolved_oxygen"] <= do_range[1])
        & (df_raw["distance_km"] <= max_distance)
    ].copy()

    if df.empty:
        st.warning("No records match the current filters.")
        return

    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Sightings",    f"{len(df):,}")
    col2.metric("Avg DO",       f"{df['avg_dissolved_oxygen'].mean():.2f}")
    col3.metric("Avg Dist (km)",f"{df['distance_km'].mean():.1f}")
    col4.metric("% Healthy DO", f"{(df['avg_dissolved_oxygen'] >= 6.0).mean()*100:.1f}%")
    col5.metric("Stations",     df["nearest_station"].nunique())
    n_anomalies = int((df["anomaly_score"] == -1).sum()) if "anomaly_score" in df.columns else 0
    anomaly_pct = (n_anomalies / len(df) * 100) if len(df) > 0 else 0
    col6.metric("Anomaly %", f"{anomaly_pct:.1f}%")

    st.markdown("---")

    # Anomaly Detection section
    st.subheader("Isolation Forest Anomaly Detection")
    st.caption("Records flagged as anomalous (-1) by EPA region-normalized Isolation Forest. Contamination=5%.")

    if "anomaly_score" in df.columns:
        df_anomaly = df[df["anomaly_score"] == -1].copy()
        df_normal  = df[df["anomaly_score"] == 1].copy()

        anom_map_col, anom_table_col = st.columns([3, 2])

        with anom_map_col:
            st.subheader("Anomaly Map")
            st.caption("Each point = one beaver sighting. Red = anomalous water quality relative to EPA climate region baseline (unusual DO/temp/pH/turbidity combination). Green = normal.")

            df_normal["color"]  = [[0, 200, 100, 120]]  * len(df_normal)
            df_anomaly["color"] = [[255, 50, 50, 220]]  * len(df_anomaly)
            df_map = pd.concat([df_normal, df_anomaly])

            anom_layer = pdk.Layer(
                "ScatterplotLayer", data=df_map,
                get_position=["decimal_longitude", "decimal_latitude"],
                get_color="color", get_radius=8000,
                pickable=True, opacity=0.9,
            )

            anom_tooltip = {
                "html": (
                    "<b>Anomaly Score:</b> {anomaly_score}<br/>"
                    "<b>State:</b> {state_province}<br/>"
                    "<b>DO:</b> {avg_dissolved_oxygen} mg/L<br/>"
                    "<b>Temp:</b> {avg_water_temp} °C<br/>"
                    "<b>pH:</b> {avg_ph}<br/>"
                    "<b>Turbidity:</b> {avg_turbidity} FNU"
                ),
                "style": {
                    "backgroundColor": "#0d0d0d",
                    "color": "#00ff9f",
                    "fontSize": "12px",
                    "padding": "8px",
                    "borderRadius": "4px",
                    "border": "1px solid #00ff9f",
                    "fontFamily": "Consolas, monospace",
                },
            }

            st.pydeck_chart(pdk.Deck(
                layers=[anom_layer],
                initial_view_state=pdk.ViewState(
                    latitude=df["decimal_latitude"].mean(),
                    longitude=df["decimal_longitude"].mean(),
                    zoom=4, pitch=0,
                ),
                tooltip=anom_tooltip,
                map_style="mapbox://styles/mapbox/dark-v11",
            ))

        with anom_table_col:
            st.subheader("Top Anomalous Stations")
            st.caption("Stations with the most flagged records")

            if len(df_anomaly) > 0:
                top_anom = (
                    df_anomaly.groupby("nearest_station")
                    .agg(
                        flagged_records=("anomaly_score", "count"),
                        avg_do=("avg_dissolved_oxygen", "mean"),
                        avg_temp=("avg_water_temp", "mean"),
                        avg_ph=("avg_ph", "mean"),
                        avg_turb=("avg_turbidity", "mean"),
                        state=("state_province", "first"),
                    )
                    .reset_index()
                    .sort_values("flagged_records", ascending=False)
                    .head(15)
                    .rename(columns={
                        "nearest_station": "Station",
                        "flagged_records": "Flagged",
                        "avg_do": "DO",
                        "avg_temp": "Temp°C",
                        "avg_ph": "pH",
                        "avg_turb": "Turb",
                        "state": "State",
                    })
                )
                top_anom["DO"]     = top_anom["DO"].round(2)
                top_anom["Temp°C"] = top_anom["Temp°C"].round(1)
                top_anom["pH"]     = top_anom["pH"].round(2)
                top_anom["Turb"]   = top_anom["Turb"].round(1)
                st.dataframe(top_anom, use_container_width=True, height=400)

                st.info(
                    f"**{len(df_anomaly):,} anomalous records** detected "
                    f"({len(df_anomaly)/len(df)*100:.1f}% of filtered data). "
                    "These stations show unusual combinations of water quality parameters "
                    "relative to their EPA climate region baseline — potential pollution "
                    "events or habitat degradation worth investigating."
                )
            else:
                st.success("No anomalies detected in current filtered data!")
    else:
        st.info("Anomaly scores not yet computed. Run the full pipeline to generate scores.")

    st.markdown("---")

    # Map + bar chart
    map_col, bar_col = st.columns([3, 2])

    with map_col:
        st.subheader("Beaver Sightings - colored by Dissolved Oxygen")
        st.caption("Low DO  ->  High DO  |  Healthy threshold: 6.0 mg/L")

        df["color"] = df["avg_dissolved_oxygen"].apply(do_to_rgb)

        layer = pdk.Layer(
            "ScatterplotLayer", data=df,
            get_position=["decimal_longitude", "decimal_latitude"],
            get_color="color", get_radius=8000,
            pickable=True, opacity=0.9, stroked=True, line_width_min_pixels=1,
        )

        view_state = pdk.ViewState(
            latitude=df["decimal_latitude"].mean(),
            longitude=df["decimal_longitude"].mean(),
            zoom=5, pitch=0,
        )

        tooltip = {
            "html": (
                "<b>Species:</b> {species}<br/>"
                "<b>State:</b> {state_province}<br/>"
                "<b>Dissolved O2:</b> {avg_dissolved_oxygen} mg/L<br/>"
                "<b>Nearest Station:</b> {nearest_station}<br/>"
                "<b>Distance:</b> {distance_km} km"
            ),
            "style": {
                "backgroundColor": "#0d0d0d",
                "color": "#00ff9f",
                "fontSize": "12px",
                "padding": "8px",
                "borderRadius": "4px",
                "border": "1px solid #00ff9f",
                "fontFamily": "Consolas, monospace",
            },
        }

        st.pydeck_chart(pdk.Deck(
            layers=[layer], initial_view_state=view_state, tooltip=tooltip,
            map_style="mapbox://styles/mapbox/dark-v11",
        ))

    with bar_col:
        st.subheader("Avg Dissolved Oxygen by State")
        st.caption("Dashed line = 6.0 mg/L healthy threshold")

        state_do = (
            df.groupby("state_province")["avg_dissolved_oxygen"].mean()
            .reset_index()
            .sort_values("avg_dissolved_oxygen", ascending=True)
            .rename(columns={"state_province": "State", "avg_dissolved_oxygen": "Avg DO (mg/L)"})
        )

        fig_bar = px.bar(
            state_do, x="Avg DO (mg/L)", y="State", orientation="h",
            color="Avg DO (mg/L)",
            color_continuous_scale=["#E6A800", "#00ff9f"],
            range_color=[6.0, 12.0],
        )
        fig_bar.add_vline(x=6.0, line_dash="dash", line_color="#555",
                          annotation_text="6.0 threshold", annotation_font_color="#888")
        fig_bar = dark_layout(fig_bar, height=420)
        fig_bar.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # Scatter + key findings
    scatter_col, stats_col = st.columns([3, 2])

    with scatter_col:
        st.subheader("Distance to Monitoring Station vs Dissolved Oxygen")
        st.caption("Each point = one beaver sighting. Color = state.")

        n_states = df["state_province"].nunique()
        neon_palette = px.colors.sample_colorscale(
            [[0, "#00ff9f"], [0.5, "#00cfff"], [1, "#a0ff00"]], n_states
        )

        fig_scatter = px.scatter(
            df, x="distance_km", y="avg_dissolved_oxygen",
            color="state_province", opacity=0.75,
            color_discrete_sequence=neon_palette,
            labels={
                "distance_km": "Distance to Nearest Station (km)",
                "avg_dissolved_oxygen": "Dissolved Oxygen (mg/L)",
                "state_province": "State",
            },
            hover_data=["nearest_station", "year"],
        )
        fig_scatter.add_hline(y=6.0, line_dash="dash", line_color="#555",
                              annotation_text="6.0 healthy threshold",
                              annotation_font_color="#888")
        fig_scatter = dark_layout(fig_scatter, height=370)
        st.plotly_chart(fig_scatter, use_container_width=True)

    with stats_col:
        st.subheader("Key Findings")

        pct_healthy     = (df["avg_dissolved_oxygen"] >= 6.0).mean() * 100
        top_state       = df["state_province"].value_counts().index[0]
        top_state_count = df["state_province"].value_counts().iloc[0]
        corr            = df["distance_km"].corr(df["avg_dissolved_oxygen"])

        st.markdown(f"""
| Metric | Value |
|--------|-------|
| Records above 6.0 mg/L | **{pct_healthy:.1f}%** |
| Top beaver state | **{top_state}** ({top_state_count} sightings) |
| DO range near beavers | **{df['avg_dissolved_oxygen'].min():.1f} - {df['avg_dissolved_oxygen'].max():.1f} mg/L** |
| Correlation (distance x DO) | **{corr:.3f}** |
| Min distance to station | **{df['distance_km'].min():.1f} km** |
| Max distance to station | **{df['distance_km'].max():.1f} km** |
""")

        st.info(
            "**Interpretation:** Dissolved oxygen levels near beaver sightings are "
            "consistently healthy (>6.0 mg/L). The weak distance correlation suggests beavers "
            "broadly associate with healthy water, independent of proximity to monitoring infrastructure."
        )

    st.markdown("---")

    st.markdown("---")

    # Water quality parameters section
    st.subheader("Additional Water Quality Parameters")
    st.caption("Temperature, pH, and turbidity near beaver sightings — 2020-2024 USGS data")

    temp_col, ph_col = st.columns(2)

    with temp_col:
        st.subheader("Avg Water Temperature by State")
        st.caption("°C — beavers prefer cooler water")

        df_temp = df[df["avg_water_temp"].notna()]
        state_temp = (
            df_temp.groupby("state_province")["avg_water_temp"].mean()
            .reset_index()
            .sort_values("avg_water_temp", ascending=True)
            .rename(columns={"state_province": "State", "avg_water_temp": "Avg Temp (°C)"})
        )

        fig_temp = px.bar(
            state_temp, x="Avg Temp (°C)", y="State", orientation="h",
            color="Avg Temp (°C)",
            color_continuous_scale=["#00cfff", "#E6A800"],
            range_color=[5.0, 25.0],
        )
        fig_temp = dark_layout(fig_temp, height=420)
        fig_temp.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_temp, use_container_width=True)

    with ph_col:
        st.subheader("Avg pH by State")
        st.caption("Healthy range: 6.5 - 8.5")

        df_ph = df[df["avg_ph"].notna()]
        state_ph = (
            df_ph.groupby("state_province")["avg_ph"].mean()
            .reset_index()
            .sort_values("avg_ph", ascending=True)
            .rename(columns={"state_province": "State", "avg_ph": "Avg pH"})
        )

        fig_ph = px.bar(
            state_ph, x="Avg pH", y="State", orientation="h",
            color="Avg pH",
            color_continuous_scale=["#E6A800", "#00ff9f"],
            range_color=[6.0, 9.0],
        )
        fig_ph.add_vline(x=6.5, line_dash="dash", line_color="#555",
                         annotation_text="6.5 min healthy", annotation_font_color="#888")
        fig_ph.add_vline(x=8.5, line_dash="dash", line_color="#555",
                         annotation_text="8.5 max healthy", annotation_font_color="#888")
        fig_ph = dark_layout(fig_ph, height=420)
        fig_ph.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_ph, use_container_width=True)

    st.markdown("---")

    # Turbidity histogram
    turb_col, turb_stats_col = st.columns([3, 2])

    with turb_col:
        st.subheader("Turbidity Distribution Near Beaver Sightings")
        st.caption("FNU — lower = clearer water. Beaver dams can increase turbidity.")

        df_turb = df[df["avg_turbidity"].notna()]

        fig_turb = px.histogram(
            df_turb, x="avg_turbidity",
            nbins=50,
            color_discrete_sequence=["#00ff9f"],
            labels={"avg_turbidity": "Turbidity (FNU)"},
        )
        fig_turb.add_vline(x=df_turb["avg_turbidity"].median(), line_dash="dash",
                           line_color="#00cfff",
                           annotation_text=f"median: {df_turb['avg_turbidity'].median():.1f}",
                           annotation_font_color="#00cfff")
        fig_turb = dark_layout(fig_turb, height=350)
        st.plotly_chart(fig_turb, use_container_width=True)

    with turb_stats_col:
        st.subheader("Water Quality Summary")
        df_temp2 = df[df["avg_water_temp"].notna()]
        df_ph2 = df[df["avg_ph"].notna()]
        df_turb2 = df[df["avg_turbidity"].notna()]

        st.markdown(f"""
| Parameter | Avg | Coverage |
|-----------|-----|----------|
| Water Temp (°C) | **{df_temp2['avg_water_temp'].mean():.1f}** | {len(df_temp2)/len(df)*100:.0f}% of records |
| pH | **{df_ph2['avg_ph'].mean():.2f}** | {len(df_ph2)/len(df)*100:.0f}% of records |
| Turbidity (FNU) | **{df_turb2['avg_turbidity'].mean():.1f}** | {len(df_turb2)/len(df)*100:.0f}% of records |
""")

        st.info(
            "**Water Quality Context:** Average temperature of 14°C reflects cool waterways "
            "preferred by beaver habitat. pH of 7.69 indicates neutral-slightly alkaline water, "
            "ideal for aquatic ecosystems. Turbidity of 25 FNU suggests moderately clear water "
            "near beaver activity zones."
        )



    # Raw data
    with st.expander("[ View Raw Data ]", expanded=False):
        st.dataframe(
            df.drop(columns=["color"], errors="ignore")
              .sort_values("avg_dissolved_oxygen", ascending=False)
              .reset_index(drop=True),
            use_container_width=True, height=300,
        )
        st.caption(f"{len(df):,} rows shown based on current filters.")

    # Footer
    st.markdown(
        "<div style='text-align:center; color:#444; font-size:0.72rem; "
        "padding-top:1.5rem; font-family: Consolas, monospace;'>"
        "Built by Yvonne Hong &nbsp;|&nbsp; "
        "<a href='https://github.com/yvnnhong/beaver-watershed-pipeline' style='color:#00ff9f;'>GitHub</a>"
        " &nbsp;|&nbsp; Data: GBIF + USGS &nbsp;|&nbsp; "
        "Pipeline: AWS Lambda -> Step Functions -> S3 -> RDS PostgreSQL -> Streamlit"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()