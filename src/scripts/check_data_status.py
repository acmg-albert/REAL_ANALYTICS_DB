"""Script to check data status in both table and materialized view."""

import logging
import sys
from pathlib import Path

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for checking data status."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client with service role key
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key  # 使用service_role_key
        )
        
        logger.info("=== 数据状态检查 ===")
        
        # 检查原始表数据
        logger.info("\n检查原始表数据...")
        response = supabase.client.table('apartment_list_time_on_market').select('*').execute()
        if response.data:
            logger.info(f"原始表记录数: {len(response.data)}")
            
            # 统计location_type分布
            type_counts = {}
            for record in response.data:
                location_type = record['location_type']
                type_counts[location_type] = type_counts.get(location_type, 0) + 1
                
            logger.info("\n原始表数据分布:")
            for location_type, count in type_counts.items():
                logger.info(f"{location_type}: {count} 条记录")
                
            # 显示示例数据
            logger.info("\n原始表示例数据:")
            for record in response.data[:3]:
                logger.info(f"Location: {record['location_name']} ({record['location_type']})")
                logger.info(f"FIPS: {record['location_fips_code']}")
                logger.info(f"Year-Month: {record['year_month']}")
                logger.info(f"Time on Market: {record['time_on_market']}")
                logger.info("---")
        else:
            logger.warning("原始表中没有数据")
            
        # 检查物化视图数据
        logger.info("\n检查物化视图数据...")
        response = supabase.client.from_('db_view_apartment_list_time_on_market_1_3').select('*').execute()
        if response.data:
            logger.info(f"物化视图记录数: {len(response.data)}")
            
            # 统计location_type分布
            type_counts = {}
            for record in response.data:
                location_type = record['location_type']
                type_counts[location_type] = type_counts.get(location_type, 0) + 1
                
            logger.info("\n物化视图数据分布:")
            for location_type, count in type_counts.items():
                logger.info(f"{location_type}: {count} 条记录")
                
            # 显示示例数据
            logger.info("\n物化视图示例数据:")
            for record in response.data[:3]:
                logger.info(f"Location: {record['location_name']} ({record['location_type']})")
                logger.info(f"FIPS: {record['location_fips_code']}")
                logger.info(f"Year-Month: {record['year_month']}")
                logger.info(f"Time on Market: {record['time_on_market']}")
                logger.info("---")
        else:
            logger.warning("物化视图中没有数据")
        
        return 0
        
    except Exception as e:
        logger.exception("检查数据时发生错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 