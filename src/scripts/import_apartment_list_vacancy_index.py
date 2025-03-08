"""Script to import ApartmentList vacancy index data into Supabase."""

import logging
import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

from ..utils.config import Config
from ..utils.exceptions import ConfigurationError, DataValidationError
from ..database.apartment_list.vacancy_index_client import VacancyIndexClient

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
    """Find the latest processed vacancy index file."""
    # 首先检查processed目录
    processed_dir = data_dir / 'processed'
    if processed_dir.exists():
        csv_files = list(processed_dir.glob("vacancy_index_processed_*.csv"))
        if csv_files:
            return max(csv_files, key=lambda p: p.stat().st_mtime)
    
    # 如果processed目录没有文件，检查data目录
    csv_files = list(data_dir.glob("vacancy_index_processed_*.csv"))
    if csv_files:
        return max(csv_files, key=lambda p: p.stat().st_mtime)
    
    raise FileNotFoundError(f"No processed vacancy index files found in {data_dir} or {processed_dir}")

def validate_data(df: pd.DataFrame) -> None:
    """Validate the vacancy index data before import."""
    # Check required columns
    required_columns = ['location_name', 'location_type', 'location_fips_code', 
                       'population', 'state', 'county', 'metro', 'year_month', 
                       'vacancy_index']
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise DataValidationError(f"Missing required columns: {missing_columns}")
    
    # 转换数据类型
    df['location_fips_code'] = df['location_fips_code'].astype(str)
    df['population'] = df['population'].fillna(0).astype(int)
    df['state'] = df['state'].fillna('').astype(str)
    df['county'] = df['county'].fillna('').astype(str)
    df['metro'] = df['metro'].fillna('').astype(str)
    
    # 检查必需字段是否有空值
    required_non_null = ['location_name', 'location_type', 'location_fips_code', 'year_month']
    for col in required_non_null:
        if df[col].isna().any():
            raise DataValidationError(f"Found null values in required column: {col}")
    
    # 检查vacancy_index的有效值范围（允许空值）
    valid_values = df[df['vacancy_index'].notna()]
    if not valid_values.empty:
        if (valid_values['vacancy_index'] < 0).any() or (valid_values['vacancy_index'] > 1).any():
            raise DataValidationError("vacancy_index values must be between 0 and 1")

def import_data_in_batches(df: pd.DataFrame, client: VacancyIndexClient, batch_size: int = 1000) -> int:
    """Import data into Supabase in batches."""
    total_imported = 0
    total_rows = len(df)
    
    with tqdm(total=total_rows, desc="Importing data") as pbar:
        for start_idx in range(0, total_rows, batch_size):
            try:
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # 处理NaN值
                batch_df = batch_df.replace({float('nan'): None, 'nan': None})
                
                # 确保location_fips_code是字符串类型
                batch_df['location_fips_code'] = batch_df['location_fips_code'].astype(str)
                
                # Convert batch to records
                records = batch_df.to_dict('records')
                
                # Insert batch
                rows_imported = client.insert_records(records)
                total_imported += rows_imported
                pbar.update(end_idx - start_idx)
                
                logger.debug(f"Imported batch {start_idx//batch_size + 1}, "
                           f"rows {start_idx+1} to {end_idx}")
                
            except Exception as e:
                logger.error(f"Error importing batch {start_idx//batch_size + 1}: {str(e)}")
                logger.error(f"First record in failed batch: {records[0] if records else 'No records'}")
                raise
    
    return total_imported

def main():
    """Main entry point for the ApartmentList vacancy index import script."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Find latest processed file
        data_dir = Path(config.data_dir)
        input_file = find_latest_processed_file(data_dir)
        logger.info(f"Processing file: {input_file}")
        
        # Read and validate data
        df = pd.read_csv(input_file)
        validate_data(df)
        logger.info(f"Data validation passed. Found {len(df)} records")
        
        # Initialize Supabase client with service role key
        if not config.supabase_service_role_key:
            raise ConfigurationError("SUPABASE_SERVICE_ROLE_KEY is required for data import")
            
        client = VacancyIndexClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # Import data
        total_imported = import_data_in_batches(df, client)
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