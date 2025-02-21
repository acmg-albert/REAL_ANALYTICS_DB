"""Script to refresh materialized views."""

import logging
import sys
from pathlib import Path

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for refreshing materialized views."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 刷新视图
        logger.info("刷新物化视图...")
        refresh_sql = "REFRESH MATERIALIZED VIEW db_view_apartment_list_time_on_market_1_3;"
        result = supabase.client.rpc('raw_sql', {'command': refresh_sql}).execute()
        
        if result.data and result.data.get('status') == 'success':
            logger.info("成功刷新物化视图")
            
            # 检查视图数据
            logger.info("检查视图数据...")
            response = supabase.client.from_('db_view_apartment_list_time_on_market_1_3').select('*').execute()
            
            if response.data:
                logger.info(f"物化视图记录数: {len(response.data)}")
                
                # 统计location_type分布
                type_counts = {}
                for record in response.data:
                    location_type = record['location_type']
                    type_counts[location_type] = type_counts.get(location_type, 0) + 1
                    
                logger.info("\n数据分布:")
                for location_type, count in type_counts.items():
                    logger.info(f"{location_type}: {count} 条记录")
            else:
                logger.warning("物化视图中没有数据")
            
            return 0
        else:
            logger.error(f"刷新视图失败: {result.data}")
            return 1
            
    except Exception as e:
        logger.exception("发生意外错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 