"""Supabase client wrapper for database operations."""

import logging
from typing import Dict, List, Optional, Union, Any
from supabase import create_client, Client
from datetime import datetime
import pytz
from ..utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class SupabaseClient:
    """Wrapper class for Supabase client with custom methods."""
    
    def __init__(self, url: str, key: str):
        """Initialize Supabase client.
        
        Args:
            url: Supabase project URL
            key: Supabase service role key or anon key
        """
        self.url = url
        self.key = key
        self.client: Client = create_client(url, key)
        logger.info(f"Initializing Supabase client with URL: {url}")
        logger.info(f"Key type: {'service_role' if len(key) > 200 else 'anon'}")
        logger.info("Supabase client initialized successfully")
    
    def execute_sql(self, sql: str) -> Dict:
        """Execute raw SQL query.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Dict containing the query result
        """
        logger.info("Executing SQL query")
        try:
            # 首先尝试使用 raw_sql 函数
            result = self.client.rpc('raw_sql', {'command': sql}).execute()
            return result.data
        except Exception as e:
            logger.warning(f"Failed to execute SQL using raw_sql function: {e}")
            logger.info("Attempting to create raw_sql function...")
            
            # 创建 raw_sql 函数
            create_function_sql = """
            -- 设置搜索路径到 public schema
            SET search_path = public;
            
            -- 删除已存在的函数（如果有）
            DROP FUNCTION IF EXISTS raw_sql(text);
            
            -- 创建新的函数
            CREATE OR REPLACE FUNCTION raw_sql(command text)
            RETURNS json
            LANGUAGE plpgsql
            SECURITY DEFINER
            SET search_path = public
            AS $$
            DECLARE
                result json;
                error_message text;
                error_detail text;
            BEGIN
                -- 尝试执行命令
                EXECUTE command;
                result := '{"status": "success"}'::json;
                RETURN result;
            EXCEPTION WHEN others THEN
                -- 获取错误信息
                GET STACKED DIAGNOSTICS 
                    error_message = MESSAGE_TEXT,
                    error_detail = PG_EXCEPTION_DETAIL;
                    
                -- 返回错误信息
                result := json_build_object(
                    'status', 'error',
                    'message', error_message,
                    'detail', error_detail
                );
                RETURN result;
            END;
            $$;
            
            -- 授予执行权限
            GRANT EXECUTE ON FUNCTION raw_sql(text) TO postgres, authenticated, anon;
            """
            
            # 直接执行 SQL 来创建函数
            try:
                # 使用 REST API 直接执行 SQL
                headers = {
                    'apikey': self.key,
                    'Authorization': f'Bearer {self.key}',
                    'Content-Type': 'application/json',
                    'Prefer': 'return=minimal'
                }
                
                response = self.client.rest.post(
                    'rest/v1/sql',
                    headers=headers,
                    json={'query': create_function_sql}
                )
                
                if response.status_code >= 400:
                    raise Exception(f"HTTP {response.status_code}: {response.text}")
                    
                logger.info("Successfully created raw_sql function")
                
                # 再次尝试执行原始 SQL
                result = self.client.rpc('raw_sql', {'command': sql}).execute()
                return result.data
                
            except Exception as e2:
                logger.error(f"Failed to create raw_sql function: {e2}")
                raise
    
    def insert_rent_estimates(self, records: List[Dict]) -> Dict:
        """Insert or update rent estimates records into Supabase.
        
        Args:
            records: List of rent estimates records to insert or update
            
        Returns:
            Dict containing the upsert operation result
        """
        try:
            processed = 0
            for record in records:
                result = self.client.rpc('raw_sql', {
                    'command': """
                    INSERT INTO apartment_list_rent_estimates 
                    (location_name, location_type, location_fips_code, 
                     population, state, county, metro,
                     year_month, rent_estimate_overall, 
                     rent_estimate_1br, rent_estimate_2br)
                    VALUES 
                    (:location_name, :location_type, :location_fips_code,
                     :population, :state, :county, :metro,
                     :year_month, :rent_estimate_overall,
                     :rent_estimate_1br, :rent_estimate_2br)
                    ON CONFLICT (location_fips_code, year_month) 
                    DO UPDATE SET
                    rent_estimate_overall = CASE 
                        WHEN EXCLUDED.rent_estimate_overall IS NOT NULL THEN EXCLUDED.rent_estimate_overall 
                        ELSE apartment_list_rent_estimates.rent_estimate_overall 
                    END,
                    rent_estimate_1br = CASE 
                        WHEN EXCLUDED.rent_estimate_1br IS NOT NULL THEN EXCLUDED.rent_estimate_1br 
                        ELSE apartment_list_rent_estimates.rent_estimate_1br 
                    END,
                    rent_estimate_2br = CASE 
                        WHEN EXCLUDED.rent_estimate_2br IS NOT NULL THEN EXCLUDED.rent_estimate_2br 
                        ELSE apartment_list_rent_estimates.rent_estimate_2br 
                    END
                    RETURNING *;
                    """
                }).execute()
                
                if result.data:
                    processed += 1
                    
            return {"processed": processed}
            
        except Exception as e:
            logger.error(f"Failed to insert rent estimates: {e}")
            raise DatabaseError(f"Failed to insert rent estimates: {e}") from e
    
    def insert_vacancy_index(self, records: List[Dict]) -> Dict:
        """Insert or update vacancy index records into Supabase.
        
        Args:
            records: List of vacancy index records to insert or update
            
        Returns:
            Dict containing the upsert operation result
        """
        try:
            processed = 0
            for record in records:
                result = self.client.rpc('raw_sql', {
                    'command': """
                    INSERT INTO apartment_list_vacancy_index 
                    (location_name, location_type, location_fips_code, 
                     population, state, county, metro,
                     year_month, vacancy)
                    VALUES 
                    (:location_name, :location_type, :location_fips_code,
                     :population, :state, :county, :metro,
                     :year_month, :vacancy)
                    ON CONFLICT (location_fips_code, year_month) 
                    DO UPDATE SET
                    vacancy = CASE 
                        WHEN EXCLUDED.vacancy IS NOT NULL THEN EXCLUDED.vacancy 
                        ELSE apartment_list_vacancy_index.vacancy 
                    END
                    RETURNING *;
                    """
                }).execute()
                
                if result.data:
                    processed += 1
                    
            return {"processed": processed}
            
        except Exception as e:
            logger.error(f"Failed to insert vacancy index: {e}")
            raise DatabaseError(f"Failed to insert vacancy index: {e}") from e
    
    def insert_time_on_market(self, records: List[Dict]) -> Dict:
        """Insert or update time on market records into Supabase.
        
        Args:
            records: List of time on market records to insert or update
            
        Returns:
            Dict containing the upsert operation result
        """
        try:
            processed = 0
            for record in records:
                result = self.client.rpc('raw_sql', {
                    'command': """
                    INSERT INTO apartment_list_time_on_market 
                    (location_name, location_type, location_fips_code, 
                     population, state, county, metro,
                     year_month, time_on_market)
                    VALUES 
                    (:location_name, :location_type, :location_fips_code,
                     :population, :state, :county, :metro,
                     :year_month, :time_on_market)
                    ON CONFLICT (location_fips_code, year_month) 
                    DO UPDATE SET
                    time_on_market = CASE 
                        WHEN EXCLUDED.time_on_market IS NOT NULL THEN EXCLUDED.time_on_market 
                        ELSE apartment_list_time_on_market.time_on_market 
                    END
                    RETURNING *;
                    """
                }).execute()
                
                if result.data:
                    processed += 1
                    
            return {"processed": processed}
            
        except Exception as e:
            logger.error(f"Failed to insert time on market: {e}")
            raise DatabaseError(f"Failed to insert time on market: {e}") from e
    
    def get_latest_rent_estimate_date(self) -> Optional[str]:
        """Get the most recent date from rent estimates table.
        
        Returns:
            The latest year_month value or None if table is empty
        """
        result = self.client.table('apartment_list_rent_estimates')\
            .select('year_month')\
            .order('year_month', desc=True)\
            .limit(1)\
            .execute()
        
        if result.data:
            return result.data[0]['year_month']
        return None
    
    def get_latest_vacancy_index_date(self) -> Optional[str]:
        """Get the most recent date from vacancy index table.
        
        Returns:
            The latest year_month value or None if table is empty
        """
        result = self.client.table('apartment_list_vacancy_index')\
            .select('year_month')\
            .order('year_month', desc=True)\
            .limit(1)\
            .execute()
        
        if result.data:
            return result.data[0]['year_month']
        return None
    
    def get_latest_time_on_market_date(self) -> Optional[str]:
        """Get the most recent date from time on market table.
        
        Returns:
            The latest year_month value or None if table is empty
        """
        result = self.client.table('apartment_list_time_on_market')\
            .select('year_month')\
            .order('year_month', desc=True)\
            .limit(1)\
            .execute()
        
        if result.data:
            return result.data[0]['year_month']
        return None 