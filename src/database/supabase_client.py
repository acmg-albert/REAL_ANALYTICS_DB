"""Supabase client for database operations."""

import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import httpx
from postgrest import APIResponse
from supabase import Client, create_client
import pandas as pd

from ..utils.config import Config
from ..utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class SupabaseClient:
    """Client for interacting with Supabase database."""
    
    def __init__(self, url: str, key: str) -> None:
        """
        Initialize Supabase client.
        
        Args:
            url: Supabase project URL
            key: Supabase API key (anon or service role)
        """
        try:
            self.client = create_client(url, key)
            self.url = url
            self.key = key
            logger.info("Supabase client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise DatabaseError(f"Failed to initialize Supabase client: {e}") from e
    
    def execute_sql(self, query: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute SQL query using Supabase client.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Dict containing query results
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            # 如果有参数,替换查询中的命名参数
            if params:
                for key, value in params.items():
                    if value is None:
                        query = query.replace(f":{key}", "NULL")
                    elif isinstance(value, str):
                        query = query.replace(f":{key}", f"'{value.replace(chr(39), chr(39)+chr(39))}'")
                    else:
                        query = query.replace(f":{key}", str(value))
            
            result = self.client.rpc('raw_sql', {'command': query}).execute()
            
            if result.data and result.data.get('status') == 'error':
                raise DatabaseError(f"SQL execution failed: {result.data.get('message')}")
            
            return result.data
            
        except Exception as e:
            logger.error(f"Failed to execute SQL query: {e}")
            raise DatabaseError(f"Failed to execute SQL query: {e}") from e
    
    def check_table_exists(self, table_name: str = "apartment_list_rent_estimates") -> bool:
        """
        Check if a table exists in the database.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            result = self.execute_sql(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name = :table_name
                );
                """,
                {"table_name": table_name}
            )
            return result.get("exists", False)
            
        except Exception as e:
            logger.error(f"Failed to check table existence: {e}")
            return False
    
    def get_latest_year_month(self, table_name: str = "apartment_list_rent_estimates") -> Optional[str]:
        """
        Get the latest year_month from the database.
        
        Args:
            table_name: Name of the table to query
            
        Returns:
            Optional[str]: Latest year_month in format 'YYYY_MM' or None if no data
        """
        try:
            result = self.execute_sql(
                f"""
                SELECT MAX(year || '_' || LPAD(month::text, 2, '0')) as latest_year_month 
                FROM {table_name};
                """
            )
            return result.get("latest_year_month")
            
        except Exception as e:
            logger.error(f"Failed to get latest year_month: {e}")
            return None
    
    def get_unique_location_fips(self, table_name: str = "apartment_list_rent_estimates") -> List[str]:
        """
        Get list of unique location FIPS codes.
        
        Args:
            table_name: Name of the table to query
            
        Returns:
            List[str]: List of unique FIPS codes
        """
        try:
            result = self.execute_sql(
                f"""
                SELECT DISTINCT location_fips_code 
                FROM {table_name} 
                ORDER BY location_fips_code;
                """
            )
            return [row["location_fips_code"] for row in result]
            
        except Exception as e:
            logger.error(f"Failed to get unique FIPS codes: {e}")
            return []
    
    def refresh_materialized_views(self) -> None:
        """Refresh all materialized views."""
        try:
            # 刷新rent_estimates视图
            self.client.rpc('raw_sql', {
                'command': 'REFRESH MATERIALIZED VIEW apartment_list_rent_estimates_view;'
            }).execute()
            
            # 刷新vacancy_index视图
            self.client.rpc('raw_sql', {
                'command': 'REFRESH MATERIALIZED VIEW apartment_list_vacancy_index_view;'
            }).execute()
            
            # 刷新time_on_market视图
            self.client.rpc('raw_sql', {
                'command': 'REFRESH MATERIALIZED VIEW apartment_list_time_on_market_view;'
            }).execute()
            
            logger.info("Successfully refreshed all materialized views")
            
        except Exception as e:
            logger.error(f"Failed to refresh materialized views: {e}")
            raise DatabaseError(f"Failed to refresh materialized views: {e}") from e

    def insert_rent_estimates(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert rent estimate records into database.
        
        Args:
            records: List of rent estimate records to insert
            
        Returns:
            int: Number of records processed
            
        Raises:
            DatabaseError: If insertion fails
        """
        try:
            # 准备批量插入的值
            values_list = []
            for record in records:
                # 处理空值和字符串转义
                processed_record = {}
                for key, value in record.items():
                    if value is None:
                        processed_record[key] = 'NULL'
                    elif isinstance(value, str):
                        processed_record[key] = f"'{value.replace(chr(39), chr(39)+chr(39))}'"
                    else:
                        processed_record[key] = str(value)
                
                # 构建值字符串
                values = f"""(
                    {processed_record['location_name']}, 
                    {processed_record['location_type']}, 
                    {processed_record['location_fips_code']},
                    {processed_record['population']}, 
                    {processed_record['state']}, 
                    {processed_record['county']}, 
                    {processed_record['metro']}, 
                    {processed_record['year_month']},
                    {processed_record['rent_estimate_overall']}, 
                    {processed_record['rent_estimate_1br']}, 
                    {processed_record['rent_estimate_2br']}
                )"""
                values_list.append(values)
            
            # 构建批量插入SQL
            query = f"""
                INSERT INTO apartment_list_rent_estimates (
                    location_name, location_type, location_fips_code,
                    population, state, county, metro, year_month,
                    rent_estimate_overall, rent_estimate_1br, rent_estimate_2br
                ) VALUES 
                {','.join(values_list)}
                ON CONFLICT (location_fips_code, year_month) 
                DO UPDATE SET
                    rent_estimate_overall = EXCLUDED.rent_estimate_overall,
                    rent_estimate_1br = EXCLUDED.rent_estimate_1br,
                    rent_estimate_2br = EXCLUDED.rent_estimate_2br
                WHERE 
                    (apartment_list_rent_estimates.rent_estimate_overall IS NULL 
                     OR apartment_list_rent_estimates.rent_estimate_overall < EXCLUDED.rent_estimate_overall)
                    OR 
                    (apartment_list_rent_estimates.rent_estimate_1br IS NULL 
                     OR apartment_list_rent_estimates.rent_estimate_1br < EXCLUDED.rent_estimate_1br)
                    OR 
                    (apartment_list_rent_estimates.rent_estimate_2br IS NULL 
                     OR apartment_list_rent_estimates.rent_estimate_2br < EXCLUDED.rent_estimate_2br)
            """
            
            # 执行批量插入
            self.execute_sql(query)
            
            # 刷新物化视图
            self.refresh_materialized_views()
            logger.info(f"成功批量插入 {len(records)} 条租金估算记录")
            
            return len(records)
            
        except Exception as e:
            logger.error(f"批量插入租金估算记录失败: {e}")
            raise DatabaseError(f"批量插入租金估算记录失败: {e}") from e
    
    def get_latest_dates(self) -> Dict[str, Optional[datetime]]:
        """
        Get latest dates from various tables.
        
        Returns:
            Dict containing latest dates for each table
        """
        try:
            result = self.execute_sql(
                """
                SELECT
                    (SELECT MAX(year || '-' || LPAD(month::text, 2, '0') || '-01')::date 
                     FROM apartment_list_rent_estimates) as rent_estimates_date,
                    (SELECT MAX(year || '-' || LPAD(month::text, 2, '0') || '-01')::date 
                     FROM apartment_list_vacancy_index) as vacancy_index_date,
                    (SELECT MAX(year || '-' || LPAD(month::text, 2, '0') || '-01')::date 
                     FROM apartment_list_time_on_market) as time_on_market_date
                """
            )
            
            return {
                "rent_estimates": result.get("rent_estimates_date"),
                "vacancy_index": result.get("vacancy_index_date"),
                "time_on_market": result.get("time_on_market_date")
            }
            
        except Exception as e:
            logger.error(f"Failed to get latest dates: {e}")
            return {
                "rent_estimates": None,
                "vacancy_index": None,
                "time_on_market": None
            }

    def insert_time_on_market(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert time on market records into database.
        
        Args:
            records: List of time on market records to insert
            
        Returns:
            int: Number of records processed
            
        Raises:
            DatabaseError: If insertion fails
        """
        try:
            processed = 0
            for record in records:
                result = self.execute_sql(
                    """
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
                    time_on_market = EXCLUDED.time_on_market
                    WHERE 
                    apartment_list_time_on_market.time_on_market IS NULL 
                    OR apartment_list_time_on_market.time_on_market < EXCLUDED.time_on_market
                    RETURNING *;
                    """,
                    record
                )
                if result:
                    processed += 1
                
            # 刷新物化视图
            self.refresh_materialized_views()
                
            return processed
            
        except Exception as e:
            logger.error(f"Failed to insert time on market data: {e}")
            raise DatabaseError(f"Failed to insert time on market data: {e}") from e 