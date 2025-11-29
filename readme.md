# Tech Sector Volatility Tracker

This project is an end-to-end data pipeline and dashboard designed to track and analyze the volatility of the "Magnificent Seven" tech stocks (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA) over the last 90 days.

## What the Code Does

1. **Data Extraction**: `extract_stock_data.py` fetches daily stock data (Open, High, Low, Close, Volume) for the specified tech stocks using the `yfinance` library and saves it to a CSV file (`tech_stocks_raw.csv`).

2. **Data Ingestion**: **Fivetran** picks up the CSV file from Google Drive and loads it into **Snowflake** as raw data.

3. **Data Transformation**: A **dbt** project (`tech_volatility/`) transforms the raw data within Snowflake.
   - **Staging**: Cleans and deduplicates raw data.
   - **Intermediate**: Calculates daily returns and moving averages.
   - **Marts**: Aggregates volatility metrics for analysis.
   - **Tests**: Run tests (`dbt test`) to ensure:
     - **Uniqueness**: No duplicate records for Ticker + Date.
     - **Completeness**: No null values in critical columns (Price, Date, Ticker).
     - **Validity**: Boolean flags contain only valid values.

4. **Visualization**: `dashboard.py` is a **Streamlit** application that connects to Snowflake to visualize the processed data, allowing users to explore stock volatility trends.

## Setup and Installation

### 1. Prerequisites
- Python 3.11
- A Snowflake account (with Account Admin access or specific role access)
- Fivetran Account
- Git

### 2. Environment Setup
Clone the repository and set up the environment:

```bash
conda create -n dbt_env python=3.11
conda activate dbt_env
pip install -r requirements.txt
```

### 3. Snowflake Configuration

Run the `setup_snowflake.sql` script in a Snowflake Worksheet to create the necessary schema for Fivetran.

  - **Database**: `FIVETRAN_DATABASE`
  - **Schema**: `FINAL_PROJECT_CHIPMUNK`

### 4. dbt Configuration

Configure your dbt profile to connect to your Snowflake instance.
Create or update your `~/.dbt/profiles.yml` file with a profile named `tech_volatility`.

**Note:** This project uses **Key Pair Authentication**.

```yaml
tech_volatility:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_org_account_id>
      user: <your_username>
      private_key_path: "C:/Users/John/Desktop/MLDS_Course/MLDS_Q4/430_Data_Warehouse/Assignment2/rsa_key.pem"
      role: FIVETRAN_ROLE
      warehouse: FIVETRAN_WAREHOUSE
      database: FIVETRAN_DATABASE
      schema: FINAL_PROJECT_ANALYTICS_CHIPMUNK
      threads: 4
```

### 5. Streamlit Configuration

Create a `.streamlit/secrets.toml` file in the root directory to allow the dashboard to connect to Snowflake.

```toml
[connections.snowflake]
user = "<your_username>"
account = "<your_org_account_id>"
role = "FIVETRAN_ROLE"
warehouse = "FIVETRAN_WAREHOUSE"
database = "FIVETRAN_DATABASE"
schema = "FINAL_PROJECT_ANALYTICS_CHIPMUNK"
private_key_path = "C:/Users/John/Desktop/MLDS_Course/MLDS_Q4/430_Data_Warehouse/Assignment2/rsa_key.pem"
client_session_keep_alive = true
```

## Running the Pipeline

### Step 1: Extract Data

Run the Python script to download the latest stock data:

```bash
python extract_stock_data.py
```

This will generate a file named `tech_stocks_raw.csv` in your project folder.

### Step 2: Load Data (Fivetran)

1.  Upload the generated `tech_stocks_raw.csv` to the **Google Drive folder** connected to Fivetran.
2.  Log in to Fivetran and open the **Google Drive Connector**.
3.  Click **"Start Sync"** (or "Sync Now").
4.  Wait for the sync to complete. This loads the data into `FIVETRAN_DATABASE.FINAL_PROJECT_CHIPMUNK.TECH_STOCKS_RAW`.

### Step 3: Transform Data (dbt)

Navigate to the dbt project directory and run the models:

```bash
cd tech_volatility
dbt debug  # Verify connection
dbt run    # Run transformations
dbt test   # Run data quality tests
dbt docs generate # Generate documentation
dbt docs serve # View documentation
```

### Step 4: Run Dashboard

Return to the root directory and launch the Streamlit app:

```bash
cd ..
streamlit run dashboard.py
```

The dashboard will open in your default web browser at `http://localhost:8501`.

```