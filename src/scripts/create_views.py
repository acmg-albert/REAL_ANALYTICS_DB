"""Script to create materialized views."""

import logging
import sys
from pathlib import Path

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """Main entry point for creating views."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 创建rent_estimates视图
        logger.info("创建rent_estimates视图...")
        rent_estimates_sql = """
        CREATE MATERIALIZED VIEW apartment_list_rent_estimates_view AS
        SELECT 
            location_name,
            location_type,
            location_fips_code,
            population,
            state,
            county,
            metro,
            year_month,
            rent_estimate_overall,
            rent_estimate_1br,
            rent_estimate_2br,
            last_update_time
        FROM apartment_list_rent_estimates
        WITH DATA;
        
        CREATE INDEX idx_rent_estimates_view_location 
        ON apartment_list_rent_estimates_view(location_fips_code);
        
        CREATE INDEX idx_rent_estimates_view_year_month 
        ON apartment_list_rent_estimates_view(year_month);
        
        GRANT SELECT ON apartment_list_rent_estimates_view TO authenticated, anon;
        """
        
        result = supabase.client.rpc('raw_sql', {'command': rent_estimates_sql}).execute()
        if result.data and result.data.get('status') == 'success':
            logger.info("成功创建rent_estimates视图")
        
        # 创建vacancy_index视图
        logger.info("创建vacancy_index视图...")
        vacancy_index_sql = """
        CREATE MATERIALIZED VIEW apartment_list_vacancy_index_view AS
        SELECT 
            location_name,
            location_type,
            location_fips_code,
            population,
            state,
            county,
            metro,
            year_month,
            vacancy_index,
            last_update_time
        FROM apartment_list_vacancy_index
        WITH DATA;
        
        CREATE INDEX idx_vacancy_index_view_location 
        ON apartment_list_vacancy_index_view(location_fips_code);
        
        CREATE INDEX idx_vacancy_index_view_year_month 
        ON apartment_list_vacancy_index_view(year_month);
        
        GRANT SELECT ON apartment_list_vacancy_index_view TO authenticated, anon;
        """
        
        result = supabase.client.rpc('raw_sql', {'command': vacancy_index_sql}).execute()
        if result.data and result.data.get('status') == 'success':
            logger.info("成功创建vacancy_index视图")
        
        # 创建time_on_market视图
        logger.info("创建time_on_market视图...")
        time_on_market_sql = """
        CREATE MATERIALIZED VIEW apartment_list_time_on_market_view AS
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
        
        CREATE INDEX idx_time_on_market_view_location 
        ON apartment_list_time_on_market_view(location_fips_code);
        
        CREATE INDEX idx_time_on_market_view_year_month 
        ON apartment_list_time_on_market_view(year_month);
        
        GRANT SELECT ON apartment_list_time_on_market_view TO authenticated, anon;
        """
        
        result = supabase.client.rpc('raw_sql', {'command': time_on_market_sql}).execute()
        if result.data and result.data.get('status') == 'success':
            logger.info("成功创建time_on_market视图")
        
        return 0
        
    except Exception as e:
        logger.exception("创建视图时发生错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 