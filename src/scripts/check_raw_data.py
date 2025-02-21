"""Script to check raw data in the table."""

import logging
import sys
from pathlib import Path

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for checking raw data."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 检查原始表数据
        check_sql = """
        WITH total_count AS (
            SELECT COUNT(*) as count
            FROM apartment_list_time_on_market
        ),
        type_distribution AS (
            SELECT location_type, COUNT(*) as count
            FROM apartment_list_time_on_market
            GROUP BY location_type
            ORDER BY COUNT(*) DESC
        ),
        sample_data AS (
            SELECT *
            FROM apartment_list_time_on_market
            LIMIT 5
        )
        SELECT 
            (SELECT count FROM total_count) as total_count,
            (SELECT array_to_json(array_agg(row_to_json(type_distribution))) FROM type_distribution) as distribution,
            (SELECT array_to_json(array_agg(row_to_json(sample_data))) FROM sample_data) as sample_data;
        """
        
        result = supabase.client.rpc('raw_sql', {'command': check_sql}).execute()
        if result.data and result.data.get('status') == 'success':
            data = result.data.get('result', [{}])[0]
            logger.info(f"原始表中的记录数: {data.get('total_count', 0)}")
            logger.info("原始表中的数据分布:")
            for item in data.get('distribution', []):
                logger.info(f"{item['location_type']}: {item['count']} 条记录")
            logger.info("\n示例数据:")
            for item in data.get('sample_data', []):
                logger.info(f"Location: {item['location_name']} ({item['location_type']})")
                logger.info(f"FIPS: {item['location_fips_code']}")
                logger.info(f"Year-Month: {item['year_month']}")
                logger.info(f"Time on Market: {item['time_on_market']}")
                logger.info("---")
        
        return 0
        
    except Exception as e:
        logger.exception("检查数据时发生错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 