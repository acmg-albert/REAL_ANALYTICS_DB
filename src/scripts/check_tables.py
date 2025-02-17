"""Script to check database tables."""

import logging
import sys
from pathlib import Path

from ..database.supabase_client import SupabaseClient
from ..utils.config import Config
from ..utils.exceptions import DatabaseError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path("logs") / "database_check.log")
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main entry point for the database checking script."""
    try:
        # Load configuration
        config = Config.from_env()
        logger.info("Configuration loaded successfully")

        # Create Supabase client
        db = SupabaseClient(config.supabase_url, config.supabase_key)
        logger.info("Supabase client created successfully")

        # Check if table exists
        logger.info("Checking if table exists...")
        if db.check_table_exists():
            # Get latest year_month
            latest_year_month = db.get_latest_year_month()
            if latest_year_month:
                logger.info(f"Latest year_month in database: {latest_year_month}")
            else:
                logger.info("No data in the table yet")
        
        return 0

    except DatabaseError as e:
        logger.error(f"Database error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 