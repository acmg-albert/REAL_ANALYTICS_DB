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
        
        # SQL to check view existence and last refresh time
        sql = """
        SELECT 
            mv.matviewname,
            n.nspname as schema_name,
            pg_size_pretty(pg_relation_size(c.oid)) as size,
            pg_stat_get_last_autovacuum_time(c.oid) as last_autovacuum,
            pg_stat_get_last_analyze_time(c.oid) as last_analyze,
            pg_stat_get_rows_inserted(c.oid) as rows_inserted
        FROM pg_matviews mv
        JOIN pg_class c ON c.relname = mv.matviewname
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE mv.matviewname = 'db_view_apartment_list_rent_estimates_1_3';
        """
        
        # Execute SQL using REST API
        response = db.client.rpc('raw_sql', {'command': sql}).execute()
        
        if not response.data:
            print("\n物化视图不存在！")
            print("需要在 Supabase SQL 编辑器中创建视图。")
            return
            
        view_info = response.data[0]
        print("\n物化视图状态：")
        print("----------------------------------------")
        print(f"名称: {view_info['matviewname']}")
        print(f"模式: {view_info['schema_name']}")
        print(f"大小: {view_info['size']}")
        print(f"最后自动清理时间: {view_info['last_autovacuum']}")
        print(f"最后分析时间: {view_info['last_analyze']}")
        print(f"插入的行数: {view_info['rows_inserted']}")
        
        # 检查视图中的数据
        sql_data = """
        SELECT 
            COUNT(*) as total_records,
            MIN(year_month) as earliest_month,
            MAX(year_month) as latest_month
        FROM db_view_apartment_list_rent_estimates_1_3;
        """
        
        response = db.client.rpc('raw_sql', {'command': sql_data}).execute()
        
        if response.data:
            data_info = response.data[0]
            print("\n数据统计：")
            print("----------------------------------------")
            print(f"总记录数: {data_info['total_records']}")
            print(f"最早月份: {data_info['earliest_month']}")
            print(f"最新月份: {data_info['latest_month']}")
            
    except Exception as e:
        print(f"错误: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(check_materialized_view()) 