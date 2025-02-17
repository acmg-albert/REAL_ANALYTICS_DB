"""Script to check the downloaded data."""

import sys
from pathlib import Path

import pandas as pd

def main():
    """Main entry point for the data checking script."""
    # Get the most recent CSV file
    data_dir = Path("data")
    csv_files = list(data_dir.glob("rent_estimates_*.csv"))
    if not csv_files:
        print("No CSV files found")
        return 1
        
    latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
    print(f"Reading {latest_file}")
    
    # Read the data
    df = pd.read_csv(latest_file)
    
    # Print basic information
    print(f"\nShape: {df.shape}")
    print(f"\nColumns: {df.columns.tolist()}")
    print(f"\nSample data:")
    print(df.head())
    
    # Print summary statistics
    print(f"\nSummary statistics:")
    print(df.describe())
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 