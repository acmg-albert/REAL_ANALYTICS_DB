"""Script to execute SQL statements directly."""

import logging
import sys
from pathlib import Path

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for executing SQL statements."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # SQL statements to execute
        sql_statements = [
            # Drop existing view
            "DROP MATERIALIZED VIEW IF EXISTS db_view_apartment_list_time_on_market_1_3;",
            
            # Create new view
            """
            CREATE MATERIALIZED VIEW db_view_apartment_list_time_on_market_1_3 AS
            SELECT DISTINCT ON (location_fips_code, year_month)
                location_name,
                'City' as location_type,
                location_fips_code,
                population,
                state,
                county,
                metro,
                year_month,
                time_on_market,
                last_update_time
            FROM apartment_list_time_on_market
            ORDER BY location_fips_code, year_month, last_update_time DESC
            WITH DATA;
            """,
            
            # Create indexes
            """
            CREATE INDEX idx_mv_time_on_market_location 
            ON db_view_apartment_list_time_on_market_1_3(location_fips_code);
            """,
            
            """
            CREATE INDEX idx_mv_time_on_market_year_month 
            ON db_view_apartment_list_time_on_market_1_3(year_month);
            """,
            
            # Grant permissions
            """
            GRANT SELECT ON db_view_apartment_list_time_on_market_1_3 TO authenticated, anon;
            """
        ]
        
        # Execute each statement
        for i, sql in enumerate(sql_statements, 1):
            logger.info(f"执行SQL语句 {i}/{len(sql_statements)}...")
            result = supabase.client.rpc('raw_sql', {'command': sql}).execute()
            if result.data and result.data.get('status') == 'success':
                logger.info(f"SQL语句 {i} 执行成功")
            else:
                logger.error(f"SQL语句 {i} 执行失败: {result.data}")
                return 1
        
        # 检查视图数据
        check_sql = """
        WITH total_count AS (
            SELECT COUNT(*) as count
            FROM db_view_apartment_list_time_on_market_1_3
        ),
        type_distribution AS (
            SELECT location_type, COUNT(*) as count
            FROM db_view_apartment_list_time_on_market_1_3
            GROUP BY location_type
            ORDER BY COUNT(*) DESC
        )
        SELECT 
            (SELECT count FROM total_count) as total_count,
            (SELECT array_to_json(array_agg(row_to_json(type_distribution))) FROM type_distribution) as distribution;
        """
        
        result = supabase.client.rpc('raw_sql', {'command': check_sql}).execute()
        if result.data and result.data.get('status') == 'success':
            data = result.data.get('result', [{}])[0]
            logger.info(f"物化视图中的记录数: {data.get('total_count', 0)}")
            logger.info("物化视图中的数据分布:")
            for item in data.get('distribution', []):
                logger.info(f"{item['location_type']}: {item['count']} 条记录")
        
        return 0
        
    except Exception as e:
        logger.exception("执行SQL时发生错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 