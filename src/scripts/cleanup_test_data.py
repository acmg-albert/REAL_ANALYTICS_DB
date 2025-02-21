"""Script to clean up test data from database."""

import logging
import sys
from pathlib import Path

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for cleaning up test data."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 删除测试数据
        test_fips = ['12345', '67890']
        
        # 从原始表删除
        logger.info("从原始表删除测试数据...")
        for fips in test_fips:
            response = supabase.client.table('apartment_list_time_on_market')\
                .delete()\
                .eq('location_fips_code', fips)\
                .execute()
            if response.data:
                logger.info(f"已删除FIPS为{fips}的测试数据")
            else:
                logger.warning(f"未找到FIPS为{fips}的测试数据")
        
        # 刷新物化视图
        logger.info("刷新物化视图...")
        refresh_sql = "REFRESH MATERIALIZED VIEW db_view_apartment_list_time_on_market_1_3;"
        result = supabase.client.rpc('raw_sql', {'command': refresh_sql}).execute()
        
        if result.data and result.data.get('status') == 'success':
            logger.info("成功刷新物化视图")
            
            # 检查视图数据
            logger.info("检查视图数据...")
            response = supabase.client.from_('db_view_apartment_list_time_on_market_1_3')\
                .select('*')\
                .in_('location_fips_code', test_fips)\
                .execute()
                
            if not response.data:
                logger.info("确认测试数据已从视图中删除")
            else:
                logger.warning("视图中仍存在测试数据")
                for record in response.data:
                    logger.warning(f"残留记录: {record}")
        else:
            logger.error(f"刷新视图失败: {result.data}")
            return 1
        
        return 0
        
    except Exception as e:
        logger.exception("清理数据时发生错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 