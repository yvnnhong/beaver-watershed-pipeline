import streamlit as st
import pandas as pd
import psycopg2
import pydeck as pdk
import plotly.express as px
import numpy as np

# ── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Beaver Watershed Pipeline",
    page_icon="🦫",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Minimal custom CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 1rem; }
    .metric-label { font-size: 0.8rem; color: #888; }
    h1 { font-weight: 700; }
    h2, h3 { font-weight: 600; }
</style>
""", unsafe_allow_html=True)


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)  # cache for 1 hour
def load_data():
    """Load beaver water quality data from RDS PostgreSQL."""
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
        df = df[df["avg_dissolved_oxygen"] > 0]  # filter out bad values
        df["year"] = df["year"].astype("Int64").astype(str)
        return df
    except Exception as e:
        st.error(f"❌ Database connection failed: {e}")
        st.stop()


# ── Sidebar filters ───────────────────────────────────────────────────────────
def render_sidebar(df):
    st.sidebar.header("Filters")

    # State filter
    states = sorted(df["state_province"].dropna().unique().tolist())
    selected_states = st.sidebar.multiselect(
        "State",
        options=states,
        default=states,
        help="Filter by state/province",
    )

    # Dissolved oxygen range
    do_min = float(df["avg_dissolved_oxygen"].min())
    do_max = float(df["avg_dissolved_oxygen"].max())
    do_range = st.sidebar.slider(
        "Dissolved Oxygen (mg/L)",
        min_value=round(do_min, 1),
        max_value=round(do_max, 1),
        value=(round(do_min, 1), round(do_max, 1)),
        step=0.1,
    )

    # Distance filter
    dist_max = float(df["distance_km"].max())
    max_distance = st.sidebar.slider(
        "Max Distance to Station (km)",
        min_value=0,
        max_value=int(dist_max),
        value=int(dist_max),
        step=10,
    )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        "**About:** End-to-end AWS data pipeline ingesting GBIF beaver occurrence "
        "records and USGS dissolved oxygen readings, spatially joined and visualized here."
    )
    st.sidebar.markdown(
        "[View on GitHub](https://github.com/yvnnhong/beaver-watershed-pipeline)"
    )

    return selected_states, do_range, max_distance


# ── Helper: DO color scale (green = healthy, yellow = borderline) ─────────────
def do_to_rgb(value, vmin=6.0, vmax=12.0):
    """Map dissolved oxygen value to an RGB color for pydeck."""
    t = max(0.0, min(1.0, (value - vmin) / (vmax - vmin)))
    # Low DO → amber (230,160,0), High DO → teal (0,180,140)
    r = int(230 * (1 - t))
    g = int(160 + 20 * t)
    b = int(140 * t)
    return [r, g, b, 200]


# ── Main app ──────────────────────────────────────────────────────────────────
def main():
    # Header
    st.title("🦫 Beaver Watershed Pipeline")
    st.markdown(
        "Exploring whether beaver activity correlates with healthy dissolved oxygen "
        "levels in nearby US waterways. Data: **GBIF** (beaver occurrences) + "
        "**USGS Water Services** (dissolved oxygen). Pipeline: **AWS Lambda → S3 → RDS PostgreSQL**."
    )
    st.markdown("---")

    # Load data
    with st.spinner("Loading data from RDS..."):
        df_raw = load_data()

    # Apply sidebar filters
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

    # ── KPI row ───────────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Sightings", f"{len(df):,}")
    col2.metric("Avg Dissolved O₂", f"{df['avg_dissolved_oxygen'].mean():.2f} mg/L")
    col3.metric("Avg Distance to Station", f"{df['distance_km'].mean():.1f} km")
    col4.metric("States Represented", df["state_province"].nunique())
    col5.metric("Monitoring Stations", df["nearest_station"].nunique())

    st.markdown("---")

    # ── Map + bar chart (side by side) ────────────────────────────────────────
    map_col, bar_col = st.columns([3, 2])

    with map_col:
        st.subheader("Beaver Sightings — colored by Dissolved Oxygen")
        st.caption("🟡 Lower DO  →  🟢 Higher DO  |  Healthy threshold: 6.0 mg/L")

        # Build color column
        df["color"] = df["avg_dissolved_oxygen"].apply(do_to_rgb)

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=df,
            get_position=["decimal_longitude", "decimal_latitude"],
            get_color="color",
            get_radius=8000,
            pickable=True,
            opacity=0.85,
            stroked=True,
            line_width_min_pixels=1,
        )

        view_state = pdk.ViewState(
            latitude=df["decimal_latitude"].mean(),
            longitude=df["decimal_longitude"].mean(),
            zoom=5,
            pitch=0,
        )

        tooltip = {
            "html": (
                "<b>Species:</b> {species}<br/>"
                "<b>State:</b> {state_province}<br/>"
                "<b>Dissolved O₂:</b> {avg_dissolved_oxygen} mg/L<br/>"
                "<b>Nearest Station:</b> {nearest_station}<br/>"
                "<b>Distance:</b> {distance_km} km"
            ),
            "style": {
                "backgroundColor": "#1e1e1e",
                "color": "white",
                "fontSize": "12px",
                "padding": "8px",
                "borderRadius": "4px",
            },
        }

        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style="mapbox://styles/mapbox/light-v11",
        )
        st.pydeck_chart(deck)

    with bar_col:
        st.subheader("Avg Dissolved Oxygen by State")
        st.caption("Dashed line = 6.0 mg/L healthy threshold")

        state_do = (
            df.groupby("state_province")["avg_dissolved_oxygen"]
            .mean()
            .reset_index()
            .sort_values("avg_dissolved_oxygen", ascending=True)
            .rename(columns={"state_province": "State", "avg_dissolved_oxygen": "Avg DO (mg/L)"})
        )

        fig_bar = px.bar(
            state_do,
            x="Avg DO (mg/L)",
            y="State",
            orientation="h",
            color="Avg DO (mg/L)",
            color_continuous_scale=["#E6A800", "#00B48C"],
            range_color=[6.0, 12.0],
        )
        fig_bar.add_vline(x=6.0, line_dash="dash", line_color="#888", annotation_text="6.0 threshold")
        fig_bar.update_layout(
            margin=dict(l=0, r=10, t=10, b=10),
            coloraxis_showscale=False,
            height=400,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#eee"),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("---")

    # ── Scatter plot + summary stats ──────────────────────────────────────────
    scatter_col, stats_col = st.columns([3, 2])

    with scatter_col:
        st.subheader("Distance to Monitoring Station vs Dissolved Oxygen")
        st.caption("Each point = one beaver sighting. Color = state.")

        fig_scatter = px.scatter(
            df,
            x="distance_km",
            y="avg_dissolved_oxygen",
            color="state_province",
            opacity=0.6,
            labels={
                "distance_km": "Distance to Nearest Station (km)",
                "avg_dissolved_oxygen": "Dissolved Oxygen (mg/L)",
                "state_province": "State",
            },
            hover_data=["nearest_station", "year"],
        )
        fig_scatter.add_hline(y=6.0, line_dash="dash", line_color="#888", annotation_text="6.0 healthy threshold")
        fig_scatter.update_layout(
            margin=dict(l=0, r=10, t=10, b=10),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis=dict(gridcolor="#eee"),
            yaxis=dict(gridcolor="#eee"),
            height=350,
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    with stats_col:
        st.subheader("Key Findings")

        pct_healthy = (df["avg_dissolved_oxygen"] >= 6.0).mean() * 100
        top_state = df["state_province"].value_counts().index[0]
        top_state_count = df["state_province"].value_counts().iloc[0]
        corr = df["distance_km"].corr(df["avg_dissolved_oxygen"])

        st.markdown(f"""
| Metric | Value |
|--------|-------|
| Records above 6.0 mg/L | **{pct_healthy:.1f}%** |
| Top beaver state | **{top_state}** ({top_state_count} sightings) |
| DO range near beavers | **{df['avg_dissolved_oxygen'].min():.1f} – {df['avg_dissolved_oxygen'].max():.1f} mg/L** |
| Correlation (distance × DO) | **{corr:.3f}** |
| Min distance to station | **{df['distance_km'].min():.1f} km** |
| Max distance to station | **{df['distance_km'].max():.1f} km** |
""")

        st.info(
            "💡 **Interpretation:** Dissolved oxygen levels near beaver sightings are "
            "consistently healthy (>6.0 mg/L). The weak distance correlation suggests beavers "
            "broadly associate with healthy water, independent of proximity to monitoring infrastructure."
        )

    st.markdown("---")

    # ── Raw data expander ─────────────────────────────────────────────────────
    with st.expander("📋 View Raw Data"):
        st.dataframe(
            df.drop(columns=["color"], errors="ignore")
              .sort_values("avg_dissolved_oxygen", ascending=False)
              .reset_index(drop=True),
            use_container_width=True,
            height=300,
        )
        st.caption(f"{len(df):,} rows shown based on current filters.")

    # ── Footer ────────────────────────────────────────────────────────────────
    st.markdown(
        "<div style='text-align:center; color:#aaa; font-size:0.75rem; padding-top:1rem;'>"
        "Built by Yvonne Hong · "
        "<a href='https://github.com/yvnnhong/beaver-watershed-pipeline' style='color:#aaa;'>GitHub</a> · "
        "Data: GBIF + USGS · Pipeline: AWS Lambda → S3 → RDS PostgreSQL → Streamlit"
        "</div>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()