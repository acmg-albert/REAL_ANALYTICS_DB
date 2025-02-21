"""Script to create database views."""

import logging
import sys
from pathlib import Path

from ..utils.config import Config
from ..utils.exceptions import ConfigurationError
from ..utils.supabase_client import SupabaseClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def read_sql_file(file_path: Path) -> str:
    """Read SQL file contents.
    
    Args:
        file_path: Path to SQL file
        
    Returns:
        str: SQL file contents
    """
    with open(file_path, 'r') as f:
        return f.read()

def main():
    """Main entry point for creating database views."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client with service role key
        if not config.supabase_service_role_key:
            raise ConfigurationError("SUPABASE_SERVICE_ROLE_KEY is required for creating views")
            
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # Read and execute SQL files
        sql_dir = Path("src/database/sql")
        
        # Create raw_sql function first
        raw_sql_function = read_sql_file(sql_dir / "create_raw_sql_function.sql")
        logger.info("Creating raw_sql function...")
        try:
            supabase.execute_sql(raw_sql_function)
        except Exception as e:
            logger.warning(f"Failed to create raw_sql function: {e}")
            logger.warning("Continuing with view creation...")
        
        # Create rent estimates view
        rent_estimates_sql = read_sql_file(sql_dir / "create_materialized_view.sql")
        logger.info("Creating rent estimates view...")
        supabase.execute_sql(rent_estimates_sql)
        
        # Create vacancy index view
        vacancy_index_sql = read_sql_file(sql_dir / "create_vacancy_index_view.sql")
        logger.info("Creating vacancy index view...")
        supabase.execute_sql(vacancy_index_sql)
        
        logger.info("Successfully created database views")
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"SQL file not found: {e}")
        return 1
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 