"""Script to check time on market data in Supabase."""

import logging
import sys
from pathlib import Path

# Add the project root directory to the Python path
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.config import Config
from src.database.supabase_client import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for checking time on market data."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 获取总行数
        response = supabase.client.table('apartment_list_time_on_market').select('count').execute()
        total_rows = response.data[0]['count']
        logger.info(f"总行数: {total_rows}")
        
        # 检查测试数据
        test_data = supabase.client.table('apartment_list_time_on_market')\
            .select('*')\
            .eq('location_name', 'Test Location')\
            .execute()
            
        if test_data.data:
            logger.info("发现测试数据:")
            for record in test_data.data:
                logger.info(f"位置: {record['location_name']}, FIPS: {record['location_fips_code']}, 时间: {record['year_month']}")
        else:
            logger.info("未发现测试数据")
            
        # 检查数据分布
        distribution = supabase.client.table('apartment_list_time_on_market')\
            .select('location_type')\
            .execute()
            
        location_types = {}
        for record in distribution.data:
            location_type = record['location_type']
            location_types[location_type] = location_types.get(location_type, 0) + 1
            
        logger.info("数据分布:")
        for location_type, count in location_types.items():
            logger.info(f"{location_type}: {count} 条记录")
            
        # 检查小写"city"的记录
        lowercase_city_records = supabase.client.table('apartment_list_time_on_market')\
            .select('*')\
            .eq('location_type', 'city')\
            .execute()
            
        if lowercase_city_records.data:
            logger.info("\n小写'city'的记录:")
            for record in lowercase_city_records.data:
                logger.info(f"位置: {record['location_name']}, FIPS: {record['location_fips_code']}, 时间: {record['year_month']}")
            
        return 0
        
    except Exception as e:
        logger.exception("检查数据时发生错误")
        logger.error(str(e))
        raise

if __name__ == "__main__":
    sys.exit(main()) 