import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# --- CONFIGURATION ---
st.set_page_config(
    page_title="Underrated Cinema Gems | Hidden Discovery",
    page_icon="🎬",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- DATABASE CONNECTION ---
def get_project_root():
    """Finds the project root by searching for pipeline.yml."""
    current = Path(__file__).resolve().parent
    while current != current.parent:
        if (current / "pipeline.yml").exists():
            return current
        current = current.parent
    return Path(__file__).resolve().parent.parent # fallback

PROJECT_ROOT = get_project_root()
DB_PATH = PROJECT_ROOT / "duckdb.db"

@st.cache_data
def load_data():
    conn = duckdb.connect(str(DB_PATH))
    query = "SELECT * FROM reports.underrated_gems"
    df = conn.execute(query).df()
    conn.close()
    # Ensure start_year is integer for better plotting
    df['start_year'] = df['start_year'].astype(int)
    return df

# --- STYLING ---
def apply_custom_style():
    st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'Outfit', sans-serif;
        }
        
        /* Main Background */
        .stApp {
            background-color: #0d1117;
            color: #e6edf3;
        }
        
        /* Sidebar Styling */
        [data-testid="stSidebar"] {
            background-color: #161b22;
            border-right: 1px solid #30363d;
        }
        
        /* Glassmorphic Cards */
        div.stMetric {
            background-color: rgba(22, 27, 34, 0.7);
            border: 1px solid #30363d;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
            transition: transform 0.2s;
        }
        div.stMetric:hover {
            transform: translateY(-5px);
            border-color: #2ea043;
        }
        
        /* Titles and Accents */
        h1, h2, h3 {
            color: #58a6ff !important;
        }
        .emerald-text {
            color: #2ea043;
            font-weight: bold;
        }
        
        /* Custom Table Styling */
        .stDataFrame {
            border: 1px solid #30363d;
            border-radius: 8px;
        }
        </style>
    """, unsafe_allow_html=True)

# --- APP LAYOUT ---
def main():
    apply_custom_style()
    
    st.title("🎬 Underrated Cinema Gems")
    st.markdown("### *Discover High-Quality Treasures Hiding in the Shadows*")
    
    try:
        df = load_data()
    except Exception as e:
        st.error(f"Failed to connect to DuckDB: {e}")
        st.info(f"Checking path: {DB_PATH.absolute()}")
        return

    # --- SIDEBAR FILTERS ---
    st.sidebar.header("🔍 Find Gems")
    
    # Text Search
    search_query = st.sidebar.text_input("Find by Title", placeholder="e.g. The Godfather...")
    
    # Multiselect Filters
    all_regions = sorted(df['region'].unique().tolist())
    selected_regions = st.sidebar.multiselect("Regions", options=all_regions, default=[])
    
    all_languages = sorted(list(set([l.strip() for sublist in df['languages'].str.split(',').tolist() for l in sublist if l])))
    selected_languages = st.sidebar.multiselect("Languages", options=all_languages, default=[])

    all_genres = sorted(list(set([g.strip() for sublist in df['genres'].str.split(',').tolist() for g in sublist])))
    selected_genres = st.sidebar.multiselect("Genres", options=all_genres, default=[])

    year_range = st.sidebar.slider(
        "Release Period", 
        int(df['start_year'].min()), 
        int(df['start_year'].max()), 
        (int(df['start_year'].min()), int(df['start_year'].max()))
    )

    # --- FILTER LOADING ---
    filtered_df = df.copy()
    
    if search_query:
        filtered_df = filtered_df[filtered_df['localised_title'].str.contains(search_query, case=False, na=False) | 
                                 filtered_df['original_title'].str.contains(search_query, case=False, na=False)]
    
    if selected_regions:
        filtered_df = filtered_df[filtered_df['region'].isin(selected_regions)]
        
    if selected_languages:
        # Filter for rows that contain ANY of the selected languages
        mask_lang = filtered_df['languages'].apply(lambda x: any(lang in x for lang in selected_languages))
        filtered_df = filtered_df[mask_lang]

    if selected_genres:
        # Filter for rows that contain ANY of the selected genres
        mask_genre = filtered_df['genres'].apply(lambda x: any(genre in x for genre in selected_genres))
        filtered_df = filtered_df[mask_genre]
        
    filtered_df = filtered_df[(filtered_df['start_year'] >= year_range[0]) & (filtered_df['start_year'] <= year_range[1])]

    if filtered_df.empty:
        st.warning("🕵️‍♂️ No Gems found matching those criteria. Try expanding your search!")
    

    # --- TOP METRICS ---
    col1, col2, col3, col4 = st.columns(4)
    
    # Calculate means safely
    if not filtered_df.empty:
        avg_rating_val = f"{filtered_df['average_rating'].mean():.2f}"
        avg_votes_val = f"{int(filtered_df['num_votes'].mean()):,}"
    else:
        avg_rating_val = "N/A"
        avg_votes_val = "0"

    with col1:
        st.metric("Total Gems Found", len(filtered_df))
    with col2:
        st.metric("Avg Quality (Rating)", avg_rating_val)
    with col3:
        st.metric("Regions Represented", filtered_df['region'].nunique())
    with col4:
        st.metric("Avg Votes", avg_votes_val)

    st.divider()

    # --- VISUALIZATIONS ---
    row1_col1, row1_col2 = st.columns(2)

    with row1_col1:
        st.subheader("🌐 The Geography of Gems")
        region_dist = filtered_df.groupby('region').size().reset_index(name='count').sort_values('count', ascending=False).head(15)
        fig_region = px.bar(
            region_dist, 
            x='count', 
            y='region', 
            orientation='h',
            template="plotly_dark",
            color='count',
            color_continuous_scale="emrld",
            title="Distribution by Region (Top 15)"
        )
        fig_region.update_layout(showlegend=False, height=450)
        st.plotly_chart(fig_region, width='stretch')

    with row1_col2:
        st.subheader("⏳ The Evolution of Treasures")
        # Aggregating by year
        year_dist = filtered_df.groupby('start_year').size().reset_index(name='gem_count')
        fig_time = px.area(
            year_dist, 
            x='start_year', 
            y='gem_count',
            template="plotly_dark",
            color_discrete_sequence=['#2ea043'],
            title="Gems Discovered Over Time"
        )
        fig_time.update_traces(fillcolor='rgba(46, 160, 67, 0.3)')
        fig_time.update_layout(height=450)
        st.plotly_chart(fig_time, width='stretch')

    st.divider()

    # --- DATAFRAME ---
    st.subheader("📋 The Cinema Catalog")
    st.dataframe(
        filtered_df[['region', 'localised_title', 'start_year', 'genres', 'average_rating', 'num_votes', 'languages']]
        .sort_values('average_rating', ascending=False),
        width='stretch',
        hide_index=True
    )

if __name__ == "__main__":
    main()
