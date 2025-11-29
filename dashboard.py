import streamlit as st
import pandas as pd
import plotly.express as px
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector # Import the raw connector

# 1. Page Config
st.set_page_config(page_title="Tech Volatility Tracker", layout="wide")

st.title("📈 Tech Sector Volatility Tracker")
st.markdown("Analyzing the 'Magnificent Seven' stocks over the last 90 days.")

# 2. Key Pair Authentication & Connection Setup
# We use @st.cache_resource to ensure the connection is created only once
# and shared across users/reruns.
@st.cache_resource
def init_connection():
    try:
        # Get credentials from secrets
        # specific "snowflake" section in secrets.toml
        secrets = st.secrets["connections"]["snowflake"]
        
        key_path = secrets["private_key_path"]
        
        # Load the Private Key
        with open(key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None, # No password for unencrypted keys
                backend=default_backend()
            )

        # Create Connection using the raw connector
        # We pass the key object directly here. Since this logic is INSIDE
        # the cached function, Streamlit doesn't try to hash 'p_key'.
        conn = snowflake.connector.connect(
            user=secrets["user"],
            account=secrets["account"],
            role=secrets["role"],
            warehouse=secrets["warehouse"],
            database=secrets["database"],
            schema=secrets["schema"],
            private_key=p_key,
            client_session_keep_alive=secrets.get("client_session_keep_alive", True)
        )
        return conn

    except KeyError as e:
        st.error(f"Missing configuration in secrets.toml: {e}")
        st.stop()
    except FileNotFoundError:
        st.error(f"Could not find private key file at: {key_path}")
        st.stop()
    except Exception as e:
        st.error(f"Error initializing connection: {e}")
        st.stop()

# 3. Fetch Data
query = """
    SELECT 
        STOCK_DATE,
        TICKER,
        CLOSE_PRICE,
        MOVING_AVG_7D,
        DAILY_RETURN_PCT,
        VOLUME,
        IS_HIGH_VOLATILITY
    FROM MART_VOLATILITY_ANALYSIS
    ORDER BY STOCK_DATE DESC
"""

@st.cache_data
def load_data():
    # Get the connection from our custom function
    conn = init_connection()
    
    # Use cursor to fetch data
    try:
        cur = conn.cursor()
        cur.execute(query)
        # Efficiently fetch as Pandas DataFrame
        df = cur.fetch_pandas_all()
        
        # Ensure date is datetime
        df['STOCK_DATE'] = pd.to_datetime(df['STOCK_DATE'])
        
        # Standardize column names to uppercase just in case
        df.columns = [c.upper() for c in df.columns]
        
        return df
    except Exception as e:
        st.error(f"Query failed: {e}")
        # Return empty dataframe to prevent crash downstream
        return pd.DataFrame()

try:
    df = load_data()
    
    # Sidebar for filtering
    st.sidebar.header("Filters")
    if not df.empty:
        ticker_list = df['TICKER'].unique().tolist()
        # Default to first ticker
        selected_ticker = st.sidebar.selectbox("Select Ticker", ticker_list)
        
        # Filter data based on selection
        ticker_df = df[df['TICKER'] == selected_ticker]

        # --- ROW 1: METRICS ---
        col1, col2, col3 = st.columns(3)
        latest_row = ticker_df.iloc[0]
        latest_price = latest_row['CLOSE_PRICE']
        latest_change = latest_row['DAILY_RETURN_PCT'] * 100 if latest_row['DAILY_RETURN_PCT'] is not None else 0
        avg_vol = ticker_df['VOLUME'].mean()

        col1.metric("Latest Close Price", f"${latest_price:,.2f}", f"{latest_change:.2f}%")
        col2.metric("Avg Daily Volume", f"{avg_vol:,.0f}")
        col3.metric("Data Points", len(ticker_df))

        # --- ROW 2: TREND CHART ---
        st.subheader(f"Price Trend vs. 7-Day Moving Average ({selected_ticker})")
        chart_data = ticker_df.melt(
            id_vars=['STOCK_DATE'], 
            value_vars=['CLOSE_PRICE', 'MOVING_AVG_7D'], 
            var_name='Metric', 
            value_name='Price'
        )
        fig_trend = px.line(
            chart_data, 
            x='STOCK_DATE', 
            y='Price', 
            color='Metric',
            title="Price vs Trend",
            color_discrete_map={'CLOSE_PRICE': '#29b5e8', 'MOVING_AVG_7D': '#ff9f36'}
        )
        st.plotly_chart(fig_trend, use_container_width=True)

        # --- ROW 3: VOLATILITY ANALYSIS ---
        st.subheader("Volatility Analysis: Volume vs. Return")
        fig_vol = px.scatter(
            ticker_df,
            x='VOLUME',
            y='DAILY_RETURN_PCT',
            color='IS_HIGH_VOLATILITY',
            size='VOLUME',
            hover_data=['STOCK_DATE'],
            title="Does High Volume lead to High Volatility?",
            color_discrete_map={True: 'red', False: 'blue'}
        )
        st.plotly_chart(fig_vol, use_container_width=True)

        with st.expander("View Raw Data"):
            st.dataframe(ticker_df)
    else:
        st.warning("No data found in the table. Check your database or dbt models.")

except Exception as e:
    st.error(f"Error connecting to Snowflake: {e}")