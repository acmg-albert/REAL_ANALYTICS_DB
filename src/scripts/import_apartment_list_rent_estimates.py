"""Script to import ApartmentList rent estimates data to Supabase."""

import logging
import sys
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
from tqdm import tqdm

from ..utils.config import Config
from ..database.apartment_list.rent_estimates_client import RentEstimatesClient
from ..utils.exceptions import ConfigurationError, DataImportError, DataValidationError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data by handling NaN values and data types."""
    logger.debug(f"Initial data shape: {df.shape}")
    logger.debug(f"Columns: {df.columns.tolist()}")
    
    # Handle NaN values
    numeric_columns = [
        'rent_estimate_overall',
        'rent_estimate_1br',
        'rent_estimate_2br',
        'population'
    ]
    for col in numeric_columns:
        df[col] = df[col].replace([np.inf, -np.inf, np.nan], None)
    
    # Ensure correct data types
    df['location_fips_code'] = df['location_fips_code'].astype(str)
    df['location_name'] = df['location_name'].astype(str)
    df['location_type'] = df['location_type'].astype(str)
    df['state'] = df['state'].fillna('').astype(str)
    df['county'] = df['county'].fillna('').astype(str)
    df['metro'] = df['metro'].fillna('').astype(str)
    
    logger.debug("Data types after cleaning:")
    for col in df.columns:
        logger.debug(f"{col}: {df[col].dtype}")
    
    return df

def import_data_in_batches(df: pd.DataFrame, client: RentEstimatesClient, batch_size: int = 1000) -> int:
    """
    Import data into Supabase in batches.
    
    Args:
        df: DataFrame to import
        client: Supabase client
        batch_size: Number of records per batch
        
    Returns:
        int: Total number of records imported
        
    Raises:
        DataImportError: If import fails
    """
    total_imported = 0
    total_rows = len(df)
    
    logger.info(f"Starting batch import of {total_rows} records")
    logger.debug(f"Sample record: {df.iloc[0].to_dict() if len(df) > 0 else 'No records'}")
    
    with tqdm(total=total_rows, desc="Importing data") as pbar:
        for start_idx in range(0, total_rows, batch_size):
            try:
                end_idx = min(start_idx + batch_size, total_rows)
                batch_df = df.iloc[start_idx:end_idx]
                
                # Handle NaN values
                batch_df = batch_df.replace({float('nan'): None, 'nan': None})
                
                # Convert to records
                records = batch_df.to_dict('records')
                logger.debug(f"Batch {start_idx//batch_size + 1} sample: {records[0] if records else 'No records'}")
                
                # Import data
                rows_imported = client.insert_records(records)
                total_imported += rows_imported
                pbar.update(rows_imported)
                
                logger.debug(f"Imported batch {start_idx//batch_size + 1}, "
                           f"rows {start_idx+1} to {end_idx}, "
                           f"imported {rows_imported} records")
                
            except Exception as e:
                logger.error(f"Error importing batch {start_idx//batch_size + 1}: {str(e)}")
                logger.error(f"First record in failed batch: {records[0] if records else 'No records'}")
                raise DataImportError(f"Failed to import batch: {str(e)}") from e
    
    return total_imported

def find_latest_processed_file(data_dir: Path) -> Path:
    """Find the latest processed rent estimates file."""
    processed_files = list(data_dir.glob("rent_estimates_processed_*.csv"))
    if not processed_files:
        raise FileNotFoundError("No processed rent estimates files found")
    latest_file = max(processed_files, key=lambda p: p.stat().st_mtime)
    logger.debug(f"Found latest file: {latest_file}")
    return latest_file

def main() -> int:
    """Main entry point for importing data to Supabase."""
    try:
        # Load configuration
        config = Config.from_env()
        logger.debug("Configuration loaded successfully")
        
        # Initialize Supabase client with service role key
        if not config.supabase_service_role_key:
            raise ConfigurationError("SUPABASE_SERVICE_ROLE_KEY is required for data import")
            
        client = RentEstimatesClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        logger.debug("Supabase client initialized successfully")
        
        # Find latest processed file
        input_file = find_latest_processed_file(config.data_dir)
        logger.info(f"Processing file: {input_file}")
        
        # Read and clean data
        df = pd.read_csv(input_file)
        logger.debug(f"Read {len(df)} records from file")
        
        df = clean_data(df)
        logger.info(f"Found {len(df)} records to import")
        
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
    except DataImportError as e:
        logger.error(f"Import error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 