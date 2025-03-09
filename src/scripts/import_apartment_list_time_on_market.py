"""Script to import ApartmentList time on market data into Supabase."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import time

import pandas as pd
from tqdm import tqdm

from ..database.apartment_list.time_on_market_client import TimeOnMarketClient
from ..utils.config import Config
from ..utils.exceptions import ConfigurationError, DataValidationError, DataImportError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def find_latest_processed_file(data_dir: Path) -> Path:
    """
    Find the latest processed time on market data file.
    
    Args:
        data_dir: Data directory path
        
    Returns:
        Path: Path to the latest processed file
        
    Raises:
        FileNotFoundError: If no processed file is found
    """
    # 首先检查processed目录
    processed_dir = data_dir / 'processed'
    if processed_dir.exists():
        csv_files = list(processed_dir.glob("time_on_market_*.csv"))
        if csv_files:
            return max(csv_files, key=lambda p: p.stat().st_mtime)
            
    # 如果processed目录没有文件，检查data目录
    csv_files = list(data_dir.glob("time_on_market_processed_*.csv"))
    if csv_files:
        return max(csv_files, key=lambda p: p.stat().st_mtime)
        
    raise FileNotFoundError(f"No processed time on market files found in {data_dir}")

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform data for database import.
    
    Args:
        df: DataFrame to transform
        
    Returns:
        pd.DataFrame: Transformed DataFrame ready for import
    """
    # 如果数据已经是长格式（有year_month和time_on_market列），则不需要转换
    if 'year_month' in df.columns and 'time_on_market' in df.columns:
        df_transformed = df.copy()
    else:
        # 识别时间列（YYYY_MM格式）
        time_cols = [col for col in df.columns if '_' in col and len(col) == 7]
        
        # 转换为长格式
        id_vars = ['location_name', 'location_type', 'location_fips_code', 
                  'population', 'state', 'county', 'metro']
        df_transformed = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=time_cols,
            var_name='year_month',
            value_name='time_on_market'
        )
    
    # 处理数据类型和空值
    df_transformed['location_fips_code'] = df_transformed['location_fips_code'].astype(str)
    df_transformed['population'] = df_transformed['population'].fillna(0).astype(int)
    df_transformed['state'] = df_transformed['state'].fillna('').astype(str)
    df_transformed['county'] = df_transformed['county'].fillna('').astype(str)
    df_transformed['metro'] = df_transformed['metro'].fillna('').astype(str)
    
    # 添加last_update_time列（使用ISO格式字符串）
    df_transformed['last_update_time'] = datetime.utcnow().isoformat()
    
    return df_transformed

def validate_data(df: pd.DataFrame) -> None:
    """
    Validate the transformed data before import.
    
    Args:
        df: DataFrame to validate
        
    Raises:
        DataValidationError: If validation fails
    """
    required_cols = [
        'location_name', 'location_type', 'location_fips_code',
        'population', 'state', 'county', 'metro',
        'year_month', 'time_on_market', 'last_update_time'
    ]
    
    # 检查必需的列
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise DataValidationError(f"Missing required columns: {missing_cols}")
        
    # 检查必需字段的空值
    required_non_null = ['location_name', 'location_type', 'location_fips_code', 'year_month']
    for col in required_non_null:
        if df[col].isna().any():
            raise DataValidationError(f"Found null values in required column: {col}")
            
    # 验证year_month格式
    invalid_dates = df[~df['year_month'].str.match(r'^\d{4}_\d{2}$')]
    if not invalid_dates.empty:
        raise DataValidationError(f"Found invalid year_month format: {invalid_dates['year_month'].unique()}")
        
    # 验证time_on_market值（允许空值）
    valid_values = df[df['time_on_market'].notna()]
    if not valid_values.empty:
        if (valid_values['time_on_market'] < 0).any():
            raise DataValidationError("Found negative time on market values")

def import_data_in_batches(df: pd.DataFrame, client: TimeOnMarketClient, batch_size: int = 500) -> int:
    """Import data into Supabase in batches."""
    total_imported = 0
    total_rows = len(df)
    max_retries = 3
    retry_delay = 5  # seconds
    
    logger.info(f"Starting batch import of {total_rows} records")
    logger.debug(f"Sample record: {df.iloc[0].to_dict() if len(df) > 0 else 'No records'}")
    
    with tqdm(total=total_rows, desc="Importing data") as pbar:
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            # Handle NaN values
            batch_df = batch_df.replace({float('nan'): None, 'nan': None})
            
            # Convert to records
            records = batch_df.to_dict('records')
            
            # 重试机制
            for retry in range(max_retries):
                try:
                    logger.debug(f"Attempting batch {start_idx//batch_size + 1} (Attempt {retry + 1}/{max_retries})")
                    
                    # Import data
                    rows_imported = client.insert_records(records)
                    total_imported += rows_imported
                    pbar.update(rows_imported)
                    
                    logger.debug(f"Imported batch {start_idx//batch_size + 1}, "
                               f"rows {start_idx+1} to {end_idx}, "
                               f"imported {rows_imported} records")
                    
                    # 成功则跳出重试循环
                    break
                    
                except Exception as e:
                    if retry < max_retries - 1:
                        logger.warning(f"Error importing batch {start_idx//batch_size + 1} "
                                     f"(Attempt {retry + 1}/{max_retries}): {str(e)}")
                        logger.warning(f"Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Error importing batch {start_idx//batch_size + 1}: {str(e)}")
                        logger.error(f"First record in failed batch: {records[0] if records else 'No records'}")
                        raise DataImportError(f"Failed to import batch after {max_retries} attempts: {str(e)}")
    
    return total_imported

def main() -> int:
    """
    Main entry point for the ApartmentList time on market import script.
    
    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    try:
        # Load configuration
        config = Config.from_env()
        
        # Find latest processed file
        input_file = find_latest_processed_file(config.data_dir)
        logger.info(f"Processing file: {input_file}")
        
        # Read and validate data
        df = pd.read_csv(input_file)
        
        # Transform data
        df_transformed = transform_data(df)
        
        # Validate transformed data
        validate_data(df_transformed)
        logger.info(f"Data validation passed. Found {len(df_transformed)} records")
        
        # Initialize TimeOnMarketClient with service role key
        if not config.supabase_service_role_key:
            raise ConfigurationError("SUPABASE_SERVICE_ROLE_KEY is required for data import")
            
        client = TimeOnMarketClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # Import data
        total_imported = import_data_in_batches(df_transformed, client)
        logger.info(f"Successfully imported {total_imported} records")
        
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        return 1
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        return 1
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 