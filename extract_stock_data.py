import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def fetch_stock_data():
    # 1. Define the Magnificent Seven tickers
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']
    
    # 2. Define the date range (Last 90 days)
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    print(f"Fetching data from {start_date} to {end_date}...")

    all_data = []

    # 3. Loop through tickers and fetch data
    for ticker in tickers:
        try:
            print(f"Downloading {ticker}...")
            # download data using yfinance
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            
            # Reset index to make 'Date' a column and not the index
            data.reset_index(inplace=True)
            
            # Add a column for the Ticker symbol so we can distinguish them later
            data['Ticker'] = ticker
            
            # Append to list
            all_data.append(data)
            
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

    # 4. Combine all data into a single DataFrame
    if all_data:
        final_df = pd.concat(all_data)
        
        # 5. Clean up columns (Flatten MultiIndex if necessary and standardize names)
        # yfinance sometimes returns multi-level columns. This ensures they are flat.
        if isinstance(final_df.columns, pd.MultiIndex):
            final_df.columns = final_df.columns.get_level_values(0)
            
        # Rename columns to be database-friendly (no spaces, lowercase)
        final_df.columns = [c.lower().replace(' ', '_') for c in final_df.columns]

        # 6. Save to CSV
        output_filename = 'tech_stocks_raw.csv'
        final_df.to_csv(output_filename, index=False)
        print(f"Success! Data saved to {output_filename}")
        print(final_df.head()) # Show a preview
    else:
        print("No data fetched.")

if __name__ == "__main__":
    fetch_stock_data()