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
        
        # 要刷新的视图列表
        views = [
            'apartment_list_rent_estimates_view',
            'apartment_list_vacancy_index_view',
            'apartment_list_time_on_market_view'
        ]
        
        # 刷新所有视图
        logger.info("开始刷新物化视图...")
        for view_name in views:
            try:
                refresh_sql = f"REFRESH MATERIALIZED VIEW {view_name};"
                result = supabase.client.rpc('raw_sql', {'command': refresh_sql}).execute()
                
                if result.data and result.data.get('status') == 'success':
                    logger.info(f"成功刷新视图: {view_name}")
                    
                    # 检查视图数据
                    logger.info(f"检查视图 {view_name} 的数据...")
                    response = supabase.client.from_(view_name).select('*').execute()
                    
                    if response.data:
                        logger.info(f"视图记录数: {len(response.data)}")
                        
                        # 统计location_type分布
                        type_counts = {}
                        for record in response.data:
                            location_type = record['location_type']
                            type_counts[location_type] = type_counts.get(location_type, 0) + 1
                            
                        logger.info(f"\n{view_name} 数据分布:")
                        for location_type, count in type_counts.items():
                            logger.info(f"{location_type}: {count} 条记录")
                    else:
                        logger.warning(f"视图 {view_name} 中没有数据")
                else:
                    logger.error(f"刷新视图 {view_name} 失败: {result.data}")
            except Exception as e:
                logger.error(f"刷新视图 {view_name} 时发生错误: {str(e)}")
            
            logger.info("---")
        
        return 0
            
    except Exception as e:
        logger.exception("发生意外错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 