import logging
import sys
from pathlib import Path

# Add the project root directory to the Python path
project_root = str(Path(__file__).resolve().parents[2])
if project_root not in sys.path:
    sys.path.append(project_root)

from src.utils.config import Config
from src.database.supabase_client import SupabaseClient

# 配置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    try:
        # 加载配置
        config = Config.from_env()
        
        # 初始化 Supabase 客户端
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 删除测试数据
        test_fips = ['12345', '67890']
        for fips in test_fips:
            response = supabase.client.table('apartment_list_time_on_market')\
                .delete()\
                .eq('location_fips_code', fips)\
                .execute()
            if response.data:
                logger.info(f"已删除FIPS为{fips}的测试数据")
            else:
                logger.warning(f"未找到FIPS为{fips}的测试数据")
        
        # 检查剩余记录数
        response = supabase.client.table('apartment_list_time_on_market').select('count').execute()
        total_rows = response.data[0]['count']
        logger.info(f"清理后的总行数: {total_rows}")
        
        # 检查数据分布
        distribution = supabase.client.table('apartment_list_time_on_market')\
            .select('location_type')\
            .execute()
            
        location_types = {}
        for record in distribution.data:
            location_type = record['location_type']
            location_types[location_type] = location_types.get(location_type, 0) + 1
            
        logger.info("清理后的数据分布:")
        for location_type, count in location_types.items():
            logger.info(f"{location_type}: {count} 条记录")
            
    except Exception as e:
        logger.error("清理数据时发生错误")
        logger.error(str(e))
        raise

if __name__ == "__main__":
    main() 