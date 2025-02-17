"""Script to check materialized view status."""

import logging
import sys
from datetime import datetime

from ..utils.config import Config
from ..database.supabase_client import SupabaseClient

def check_materialized_view():
    """Check the status of the materialized view."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Create Supabase client
        db = SupabaseClient(config.supabase_url, config.supabase_key)
        
        # 获取最新数据
        print("\n最新更新的记录：")
        print("----------------------------------------")
        response = db.client.from_('db_view_apartment_list_rent_estimates_1_3')\
            .select('*')\
            .order('last_update_time', desc=True)\
            .limit(3)\
            .execute()
            
        for row in response.data:
            print(f"地区: {row['location_name']}")
            print(f"类型: {row['location_type']}")
            print(f"月份: {row['year_month']}")
            print(f"租金估计: ${row['rent_estimate_overall']}")
            print(f"更新时间: {row['last_update_time']}")
            print("----------------------------------------")
            
        # 获取最新月份的数据
        print("\n最新月份的数据示例：")
        print("----------------------------------------")
        response = db.client.from_('db_view_apartment_list_rent_estimates_1_3')\
            .select('*')\
            .order('year_month', desc=True)\
            .limit(3)\
            .execute()
            
        for row in response.data:
            print(f"地区: {row['location_name']}")
            print(f"类型: {row['location_type']}")
            print(f"月份: {row['year_month']}")
            print(f"租金估计: ${row['rent_estimate_overall']}")
            print("----------------------------------------")
            
        # 获取不同地区类型的数量
        print("\n不同地区类型统计：")
        print("----------------------------------------")
        sql = """
        SELECT location_type, COUNT(*) as count
        FROM db_view_apartment_list_rent_estimates_1_3
        GROUP BY location_type
        ORDER BY count DESC;
        """
        response = db.client.rpc('raw_sql', {'command': sql}).execute()
        
        for row in response.data:
            print(f"{row['location_type']}: {row['count']} 条记录")
            
    except Exception as e:
        print(f"错误: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(check_materialized_view()) 