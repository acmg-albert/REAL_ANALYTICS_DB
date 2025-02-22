"""Script to drop old materialized views."""

import logging
import sys
from pathlib import Path

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for dropping old views."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 要删除的旧视图
        old_views = [
            'db_view_apartment_list_rent_estimates_1_3',
            'db_view_apartment_list_vacancy_index_1_3',
            'db_view_apartment_list_time_on_market_1_3'
        ]
        
        logger.info("\n开始删除旧的物化视图:")
        for view_name in old_views:
            try:
                # 删除视图
                drop_sql = f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"
                result = supabase.client.rpc('raw_sql', {'command': drop_sql}).execute()
                
                if result.data and result.data.get('status') == 'success':
                    logger.info(f"成功删除视图: {view_name}")
                else:
                    logger.warning(f"删除视图时出现问题: {view_name}")
                    logger.warning(f"响应: {result.data}")
            except Exception as e:
                logger.error(f"删除视图 {view_name} 时发生错误: {str(e)}")
            
            logger.info("---")
        
        return 0
        
    except Exception as e:
        logger.exception("删除视图时发生错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 