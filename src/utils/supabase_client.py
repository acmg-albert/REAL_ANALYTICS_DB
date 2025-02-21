"""Supabase client wrapper for database operations."""

import logging
from typing import Dict, List, Optional, Union
from supabase import create_client, Client

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
        """Insert rent estimates records into Supabase.
        
        Args:
            records: List of rent estimate records to insert
            
        Returns:
            Dict containing the insert operation result
        """
        result = self.client.table('apartment_list_rent_estimates').insert(records).execute()
        return result.data
    
    def insert_vacancy_index(self, records: List[Dict]) -> Dict:
        """Insert or update vacancy index records into Supabase.
        
        Args:
            records: List of vacancy index records to insert or update
            
        Returns:
            Dict containing the upsert operation result
        """
        result = self.client.table('apartment_list_vacancy_index')\
            .upsert(records, on_conflict='location_fips_code,year_month')\
            .execute()
        return result.data
    
    def insert_time_on_market(self, records: List[Dict]) -> Dict:
        """Insert or update time on market records into Supabase.
        
        Args:
            records: List of time on market records to insert or update
            
        Returns:
            Dict containing the upsert operation result
        """
        result = self.client.table('apartment_list_time_on_market')\
            .upsert(records, on_conflict='location_fips_code,year_month')\
            .execute()
        return result.data
    
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