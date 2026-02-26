import pandas as pd
import quantstats as qs
import sys

def generate_report(csv_path, output_path):
    print(f"Loading returns from {csv_path}...")
    df = pd.read_csv(csv_path, index_col='date', parse_dates=True)
    
    returns = df['returns']
    returns.index = pd.to_datetime(returns.index)

    # Some data formatting checks
    if returns.empty:
        print("Error: Returns data is empty.")
        sys.exit(1)

    print(f"Generating QuantStats HTML report to {output_path}...")
    qs.reports.html(returns, output=output_path, title='Backtest Performance Report')
    print("Report generated successfully.")

if __name__ == "__main__":
    generate_report('returns.csv', 'quantstats_report.html')
