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
@st.cache_data(ttl=3600)
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
        "Exploring whether beaver activity correlates with healthy dissolved oxygen "
        "levels in nearby US waterways.  "
        "Data: **GBIF** (beaver occurrences) + **USGS Water Services** (dissolved oxygen).  "
        "Pipeline: **AWS Lambda -> S3 -> RDS PostgreSQL**."
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

    # KPI row
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Sightings",    f"{len(df):,}")
    col2.metric("Avg DO",       f"{df['avg_dissolved_oxygen'].mean():.2f}")
    col3.metric("Avg Dist (km)",f"{df['distance_km'].mean():.1f}")
    col4.metric("States",       df["state_province"].nunique())
    col5.metric("Stations",     df["nearest_station"].nunique())

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
        "Pipeline: AWS Lambda -> S3 -> RDS PostgreSQL -> Streamlit"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()