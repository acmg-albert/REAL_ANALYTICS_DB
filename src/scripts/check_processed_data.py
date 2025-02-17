"""Script to check the processed rent estimates data."""

import sys
from pathlib import Path

import pandas as pd

def main():
    """Main entry point for the processed data checking script."""
    # Get the most recent processed file
    data_dir = Path("data")
    csv_files = list(data_dir.glob("rent_estimates_processed_*.csv"))
    if not csv_files:
        print("No processed CSV files found")
        return 1
        
    latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
    print(f"Reading {latest_file}")
    
    # Read the data
    df = pd.read_csv(latest_file)
    
    # Print basic information
    print(f"\nShape: {df.shape}")
    print(f"\nColumns: {df.columns.tolist()}")
    
    # Print sample data
    print(f"\nSample data:")
    print(df.head())
    
    # Print summary for rent estimates
    print(f"\nSummary for rent estimates:")
    rent_cols = ['rent_estimate_overall', 'rent_estimate_1br', 'rent_estimate_2br']
    print(df[rent_cols].describe())
    
    # Print unique counts
    print(f"\nUnique counts:")
    for col in ['location_type', 'state', 'year_month']:
        print(f"\n{col}:")
        print(df[col].value_counts().head())
        
    return 0

if __name__ == "__main__":
    sys.exit(main()) 