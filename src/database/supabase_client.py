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
            # Handle parameters if provided
            if params:
                for key, value in params.items():
                    if value is None:
                        query = query.replace(f":{key}", "NULL")
                    elif isinstance(value, str):
                        query = query.replace(f":{key}", f"'{value.replace(chr(39), chr(39)+chr(39))}'")
                    else:
                        query = query.replace(f":{key}", str(value))
            
            result = self.client.rpc('raw_sql', {'command': query}).execute()
            
            if result.data and isinstance(result.data, dict) and result.data.get('status') == 'error':
                raise DatabaseError(f"SQL execution failed: {result.data.get('message')}")
            
            # For SELECT queries, return the result data
            if query.strip().upper().startswith('SELECT'):
                if result.data and isinstance(result.data, list):
                    if len(result.data) == 1 and isinstance(result.data[0], dict):
                        return result.data[0]
                    return result.data
                return []
            
            # For other queries, return status
            return {'status': 'success', 'affected_rows': len(result.data) if result.data else 0}
            
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
            
            # 刷新zillow_affordability视图
            self.client.rpc('raw_sql', {
                'command': 'REFRESH MATERIALIZED VIEW zillow_new_homeowner_affordability_down_20pct_view;'
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

    def insert_vacancy_index(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert vacancy index records into database.
        
        Args:
            records: List of vacancy index records to insert
            
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
                    {processed_record['vacancy_index_overall']}, 
                    {processed_record['vacancy_index_1br']}, 
                    {processed_record['vacancy_index_2br']}
                )"""
                values_list.append(values)
            
            # 构建批量插入SQL
            query = f"""
                INSERT INTO apartment_list_vacancy_index (
                    location_name, location_type, location_fips_code,
                    population, state, county, metro, year_month,
                    vacancy_index_overall, vacancy_index_1br, vacancy_index_2br
                ) VALUES 
                {','.join(values_list)}
                ON CONFLICT (location_fips_code, year_month) 
                DO UPDATE SET
                    vacancy_index_overall = CASE 
                        WHEN EXCLUDED.vacancy_index_overall IS NOT NULL THEN EXCLUDED.vacancy_index_overall 
                        ELSE apartment_list_vacancy_index.vacancy_index_overall 
                    END,
                    vacancy_index_1br = CASE 
                        WHEN EXCLUDED.vacancy_index_1br IS NOT NULL THEN EXCLUDED.vacancy_index_1br 
                        ELSE apartment_list_vacancy_index.vacancy_index_1br 
                    END,
                    vacancy_index_2br = CASE 
                        WHEN EXCLUDED.vacancy_index_2br IS NOT NULL THEN EXCLUDED.vacancy_index_2br 
                        ELSE apartment_list_vacancy_index.vacancy_index_2br 
                    END
            """
            
            # 执行批量插入
            self.execute_sql(query)
            
            # 刷新物化视图
            self.refresh_materialized_views()
            logger.info(f"成功批量插入 {len(records)} 条空置率记录")
            
            return len(records)
            
        except Exception as e:
            logger.error(f"批量插入空置率记录失败: {e}")
            raise DatabaseError(f"批量插入空置率记录失败: {e}") from e

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
                    {processed_record['time_on_market_overall']}, 
                    {processed_record['time_on_market_1br']}, 
                    {processed_record['time_on_market_2br']}
                )"""
                values_list.append(values)
            
            # 构建批量插入SQL
            query = f"""
                INSERT INTO apartment_list_time_on_market (
                    location_name, location_type, location_fips_code,
                    population, state, county, metro, year_month,
                    time_on_market_overall, time_on_market_1br, time_on_market_2br
                ) VALUES 
                {','.join(values_list)}
                ON CONFLICT (location_fips_code, year_month) 
                DO UPDATE SET
                    time_on_market_overall = CASE 
                        WHEN EXCLUDED.time_on_market_overall IS NOT NULL THEN EXCLUDED.time_on_market_overall 
                        ELSE apartment_list_time_on_market.time_on_market_overall 
                    END,
                    time_on_market_1br = CASE 
                        WHEN EXCLUDED.time_on_market_1br IS NOT NULL THEN EXCLUDED.time_on_market_1br 
                        ELSE apartment_list_time_on_market.time_on_market_1br 
                    END,
                    time_on_market_2br = CASE 
                        WHEN EXCLUDED.time_on_market_2br IS NOT NULL THEN EXCLUDED.time_on_market_2br 
                        ELSE apartment_list_time_on_market.time_on_market_2br 
                    END
            """
            
            # 执行批量插入
            self.execute_sql(query)
            
            # 刷新物化视图
            self.refresh_materialized_views()
            logger.info(f"成功批量插入 {len(records)} 条上市时间记录")
            
            return len(records)
            
        except Exception as e:
            logger.error(f"批量插入上市时间记录失败: {e}")
            raise DatabaseError(f"批量插入上市时间记录失败: {e}") from e

    def insert_zillow_affordability(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert Zillow affordability records into database.
        
        Args:
            records: List of Zillow affordability records to insert
            
        Returns:
            int: Number of records processed
            
        Raises:
            DatabaseError: If insertion fails
        """
        try:
            # Use direct upsert method instead of raw SQL
            result = self.client.table('zillow_new_homeowner_affordability_down_20pct')\
                .upsert(
                    records,
                    on_conflict='region_id,date'
                ).execute()
            
            # Refresh materialized views
            self.refresh_materialized_views()
            
            processed_count = len(result.data) if result.data else 0
            logger.info(f"Successfully inserted {processed_count} Zillow affordability records")
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Failed to insert Zillow affordability records: {e}")
            raise DatabaseError(f"Failed to insert Zillow affordability records: {e}") from e

    def get_latest_dates(self) -> Dict[str, Optional[datetime]]:
        """
        Get latest dates from various tables.
        
        Returns:
            Dict containing latest dates for each table
        """
        try:
            # Get latest dates from ApartmentList tables
            apartment_list_dates = self.execute_sql(
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
            
            # Get latest date from Zillow table
            zillow_date = self.execute_sql(
                """
                SELECT MAX(date)::date as latest_date
                FROM zillow_new_homeowner_affordability_down_20pct
                """
            )
            
            return {
                "rent_estimates": apartment_list_dates.get("rent_estimates_date"),
                "vacancy_index": apartment_list_dates.get("vacancy_index_date"),
                "time_on_market": apartment_list_dates.get("time_on_market_date"),
                "zillow_new_homeowner_affordability_down_20pct": zillow_date.get("latest_date")
            }
            
        except Exception as e:
            logger.error(f"Failed to get latest dates: {e}")
            return {
                "rent_estimates": None,
                "vacancy_index": None,
                "time_on_market": None,
                "zillow_new_homeowner_affordability_down_20pct": None
            } 