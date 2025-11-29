import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def fetch_stock_data():
    tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA']
    
    # Define Date Range
    end_date = datetime.today().strftime('%Y-%m-%d')
    start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
    
    print(f"Fetching data from {start_date} to {end_date}...")

    all_data = []

    for ticker in tickers:
        try:
            print(f"Downloading {ticker}...", end=" ")
            data = yf.download(ticker, start=start_date, end=end_date, progress=False)
            
            if data.empty:
                print("No data found.")
                continue

            # FIX: Flatten columns immediately if they are MultiIndex
            # (Recent yfinance updates sometimes return columns like ('Close', 'AAPL'))
            if isinstance(data.columns, pd.MultiIndex):
                data.columns = data.columns.get_level_values(0)

            # Reset index to ensure Date is a normal column
            data.reset_index(inplace=True)

            # Force column names to be clean string types
            data.columns = [str(c) for c in data.columns]

            # Add Ticker column
            data['Ticker'] = ticker
            
            # Append to list
            all_data.append(data)
            print(f"Success! ({len(data)} rows)")
            
        except Exception as e:
            print(f"Error: {e}")

    # Combine and Save
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Final cleanup of column names
        final_df.columns = [c.lower().replace(' ', '_') for c in final_df.columns]

        output_filename = 'tech_stocks_raw.csv'
        final_df.to_csv(output_filename, index=False)
        
        print("-" * 30)
        print(f"DONE. Total rows collected: {len(final_df)}")
        print(f"Tickers found: {final_df['ticker'].unique()}")
        print(f"Saved to {output_filename}")
    else:
        print("No data fetched.")

if __name__ == "__main__":
    fetch_stock_data()