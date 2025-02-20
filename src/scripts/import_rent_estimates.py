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

def find_latest_processed_file(data_dir: Path) -> Path:
    """Find the latest processed rent estimates file."""
    # 首先检查processed目录
    processed_dir = data_dir / 'processed'
    if processed_dir.exists():
        csv_files = list(processed_dir.glob("rent_estimates_*.csv"))
        if csv_files:
            return max(csv_files, key=lambda p: p.stat().st_mtime)
            
    # 如果processed目录没有文件，检查data目录
    csv_files = list(data_dir.glob("rent_estimates_processed_*.csv"))
    if csv_files:
        return max(csv_files, key=lambda p: p.stat().st_mtime)
        
    # 如果还是没有找到，返回默认路径
    return data_dir / 'processed' / 'rent_estimates.csv'

def main() -> int:
    """Main function to import rent estimates data."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize database client with service role key for write access
        db = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key  # 使用service_role_key进行写操作
        )
        
        # Get the data directory
        data_dir = Path(__file__).parent.parent.parent / 'data'
        if not data_dir.exists():
            logger.error(f"Data directory not found: {data_dir}")
            logger.info("Creating data directory structure...")
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / 'raw').mkdir(exist_ok=True)
            (data_dir / 'processed').mkdir(exist_ok=True)
            return 1
            
        # Find the latest processed file
        processed_file = find_latest_processed_file(data_dir)
        logger.info(f"Looking for processed file at: {processed_file}")
        
        if not processed_file.exists():
            logger.error(f"Processed data file not found: {processed_file}")
            logger.info("Available files in data directory:")
            for file in data_dir.rglob("*.csv"):
                logger.info(f"- {file.relative_to(data_dir)}")
            return 1
            
        # Read and import the data
        logger.info(f"Starting data import from {processed_file}")
        
        # Use COPY command for efficient data import
        with open(processed_file, 'r', encoding='utf-8') as f:
            db.copy_from_csv(
                table_name='apartment_list_rent_estimates',
                csv_file=f,
                columns=['location_name', 'location_type', 'location_fips_code',
                        'population', 'state', 'county', 'metro', 'year_month',
                        'rent_estimate_overall', 'rent_estimate_1br', 'rent_estimate_2br']
            )
            
        logger.info("Data import completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Failed to import data: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 