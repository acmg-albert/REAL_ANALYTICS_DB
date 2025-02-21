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
        sql_dir = Path(__file__).parent.parent / "database" / "sql"
        
        # First create raw_sql function
        logger.info("Creating raw_sql function...")
        raw_sql_function_sql = read_sql_file(sql_dir / "create_raw_sql_function.sql")
        try:
            # 使用 REST API 直接执行 SQL
            headers = {
                'apikey': config.supabase_service_role_key,
                'Authorization': f'Bearer {config.supabase_service_role_key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            }
            
            response = supabase.client.rest.post(
                'rest/v1/sql',
                headers=headers,
                json={'query': raw_sql_function_sql}
            )
            
            if response.status_code >= 400:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
                
            logger.info("Successfully created raw_sql function")
        except Exception as e:
            logger.error(f"Failed to create raw_sql function: {e}")
            return 1
        
        # Create rent estimates view
        rent_estimates_sql = read_sql_file(sql_dir / "create_materialized_view.sql")
        logger.info("Creating rent estimates view...")
        try:
            supabase.execute_sql(rent_estimates_sql)
            logger.info("Successfully created rent estimates view")
        except Exception as e:
            logger.error(f"Failed to create rent estimates view: {e}")
            logger.warning("Continuing with vacancy index view creation...")
        
        # Create vacancy index view
        vacancy_index_sql = read_sql_file(sql_dir / "create_vacancy_index_view.sql")
        logger.info("Creating vacancy index view...")
        try:
            supabase.execute_sql(vacancy_index_sql)
            logger.info("Successfully created vacancy index view")
        except Exception as e:
            logger.error(f"Failed to create vacancy index view: {e}")
            logger.warning("View creation process completed with errors")
            return 1
        
        logger.info("Successfully created all database views")
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