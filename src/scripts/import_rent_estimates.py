"""Import rent estimates from processed data into Supabase."""

import logging
import os
from pathlib import Path

from src.database import SupabaseClient
from src.utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main() -> int:
    """Main function to import rent estimates data."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize database client
        db = SupabaseClient(config.supabase_url, config.supabase_key)
        
        # Get the data directory
        data_dir = Path(__file__).parent.parent.parent / 'data'
        processed_file = data_dir / 'processed' / 'rent_estimates.csv'
        
        if not processed_file.exists():
            logger.error(f"Processed data file not found: {processed_file}")
            return 1
            
        # Read and import the data
        logger.info("Starting data import...")
        
        # Use COPY command for efficient data import
        with open(processed_file, 'r', encoding='utf-8') as f:
            db.copy_from_csv(
                table_name='apartment_list_rent_estimates',
                csv_file=f,
                columns=['city', 'state', 'bedroom_size', 'rent_estimate', 'updated_at']
            )
            
        logger.info("Data import completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Failed to import data: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 