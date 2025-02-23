"""Script to process Zillow new homeowner affordability data."""

import logging
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os
from datetime import datetime
from ..utils.logger import get_logger
from ..utils.exceptions import ConfigurationError, ProcessingError, DataValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = get_logger(__name__)

def _validate_data(df):
    """验证数据格式和内容"""
    required_columns = ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']
    
    # 检查必需列是否存在
    for col in required_columns:
        if col not in df.columns:
            raise DataValidationError(f"缺少必需列: {col}")
    
    # 验证数据类型
    if not pd.api.types.is_numeric_dtype(df['RegionID']):
        raise DataValidationError("RegionID 必须是数字类型")
    if not pd.api.types.is_numeric_dtype(df['SizeRank']):
        raise DataValidationError("SizeRank 必须是数字类型")

def main():
    """Main entry point for the Zillow affordability processing script."""
    try:
        # Get the most recent raw data file
        data_dir = Path("data")
        csv_files = list(data_dir.glob("zillow_affordability_2*.csv"))
        if not csv_files:
            logger.error("No Zillow affordability data files found")
            return 1
            
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Processing {latest_file}")
        
        # Read and validate data
        df = pd.read_csv(latest_file)
        _validate_data(df)
        
        # Get metadata columns and date columns
        metadata_cols = ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']
        date_cols = [col for col in df.columns if col not in metadata_cols]
        
        # Convert from wide to long format
        df_long = pd.melt(
            df,
            id_vars=metadata_cols,
            value_vars=date_cols,
            var_name='date',
            value_name='new_home_affordability_down_20pct'
        )
        
        # Convert date format
        df_long['date'] = pd.to_datetime(df_long['date']).dt.strftime('%Y-%m-%d')
        
        # Rename columns to match our schema
        column_mapping = {
            'RegionID': 'region_id',
            'SizeRank': 'size_rank',
            'RegionName': 'region_name',
            'RegionType': 'region_type',
            'StateName': 'state_name'
        }
        processed_df = df_long.rename(columns=column_mapping)
        
        # Handle NaN values
        processed_df['new_home_affordability_down_20pct'] = processed_df['new_home_affordability_down_20pct'].replace([np.inf, -np.inf, np.nan], None)
        
        # Ensure correct data types
        processed_df['region_id'] = processed_df['region_id'].astype(str)
        processed_df['size_rank'] = processed_df['size_rank'].astype(int)
        processed_df['region_name'] = processed_df['region_name'].astype(str)
        processed_df['region_type'] = processed_df['region_type'].astype(str)
        processed_df['state_name'] = processed_df['state_name'].fillna('').astype(str)
        
        # Sort by date and region
        processed_df = processed_df.sort_values(['date', 'size_rank'])
        
        # Generate output filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = data_dir / f"processed_zillow_affordability_{timestamp}.csv"
        
        # Save processed data
        processed_df.to_csv(output_path, index=False)
        logger.info(f"Successfully processed {len(processed_df)} records")
        logger.info(f"Data saved to: {output_path}")
        
        return 0
        
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        return 1
    except ProcessingError as e:
        logger.error(f"Processing error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 