"""Supabase database client module."""

import logging
from typing import Dict, List, Optional, Set, Tuple
from datetime import datetime
import pytz

import pandas as pd
import requests
from supabase import Client, create_client

from ..utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class SupabaseClient:
    """Client for interacting with Supabase database."""
    
    def __init__(self, url: str, key: str):
        """
        Initialize the Supabase client.
        
        Args:
            url: Supabase project URL
            key: Supabase API key (anon key for read-only, service role key for write access)
        """
        try:
            logger.info(f"Initializing Supabase client with URL: {url}")
            logger.info(f"Key type: {'service_role' if 'service_role' in key else 'anon'}")
            
            # 创建客户端
            self.client: Client = create_client(url, key)
            self.url = url.rstrip('/')
            self.key = key
            
            # 设置请求头
            self.headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            }
            
            logger.info("Supabase client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {str(e)}")
            raise DatabaseError(f"Failed to initialize Supabase client: {str(e)}") from e
        
    def execute_sql(self, sql: str) -> None:
        """
        Execute raw SQL query.
        
        Args:
            sql: SQL query to execute
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            logger.info("Executing SQL query")
            logger.debug(f"SQL: {sql}")
            
            # 使用 SQL 编辑器 API 端点
            response = requests.post(
                f"{self.url}/rest/v1/rpc/raw_sql",
                headers=self.headers,
                json={'command': sql}  # 使用 'command' 而不是 'sql'
            )
            
            if response.status_code >= 400:
                error_text = response.text if response.text else f"HTTP {response.status_code}"
                raise Exception(error_text)
                
            logger.info("SQL query executed successfully")
            
        except Exception as e:
            logger.error(f"Failed to execute SQL: {str(e)}")
            raise DatabaseError(f"Failed to execute SQL: {str(e)}") from e
            
    def get_existing_records(self, location_fips_codes: Set[str], year_months: Set[str]) -> Dict[Tuple[str, str], Dict]:
        """
        Get existing records for the given location_fips_codes and year_months.
        
        Args:
            location_fips_codes: Set of location FIPS codes
            year_months: Set of year_month values
            
        Returns:
            Dict[Tuple[str, str], Dict]: Dictionary mapping (location_fips_code, year_month) to record data
            
        Raises:
            DatabaseError: If query fails
        """
        try:
            logger.info(f"Fetching existing records for {len(location_fips_codes)} locations and {len(year_months)} months")
            logger.debug(f"Location FIPS codes: {location_fips_codes}")
            logger.debug(f"Year months: {year_months}")
            
            # 使用 Supabase 客户端查询
            response = self.client.table('apartment_list_rent_estimates')\
                .select('*')\
                .in_('location_fips_code', list(location_fips_codes))\
                .in_('year_month', list(year_months))\
                .execute()
            
            # 将结果转换为字典
            existing_records = {}
            for record in response.data:
                key = (record['location_fips_code'], record['year_month'])
                existing_records[key] = record
                
            logger.info(f"Found {len(existing_records)} existing records")
            logger.debug(f"Existing records: {existing_records}")
            return existing_records
            
        except Exception as e:
            logger.error(f"Failed to get existing records: {str(e)}")
            raise DatabaseError(f"Failed to get existing records: {str(e)}") from e
            
    def insert_rent_estimates(self, records: List[Dict]) -> int:
        """
        Insert rent estimates records into the database.
        
        处理逻辑：
        1. 新的locations：添加所有相关数据
        2. 现有locations的新月份：添加新月份数据
        3. 现有数据的更新：
           - 如果新数据不为空（非None且非0），则更新
           - 如果新数据为空，保留原有数据
        
        Args:
            records: List of rent estimates records
            
        Returns:
            int: Number of records inserted/updated
            
        Raises:
            DatabaseError: If insertion fails
        """
        try:
            logger.info(f"Processing {len(records)} records")
            
            # 获取美国东部时区
            eastern = pytz.timezone('America/New_York')
            current_time = datetime.now(eastern)
            
            # 收集所有的 location_fips_codes 和 year_months
            location_fips_codes = {r['location_fips_code'] for r in records}
            year_months = {r['year_month'] for r in records}
            
            # 获取现有记录
            existing_records = self.get_existing_records(location_fips_codes, year_months)
            
            # 处理每条记录
            records_to_upsert = []
            stats = {
                'new_locations': 0,
                'new_months': 0,
                'updated': 0,
                'preserved': 0
            }
            
            for record in records:
                key = (record['location_fips_code'], record['year_month'])
                record['last_update_time'] = current_time.isoformat()
                
                if key not in existing_records:
                    # 情况1和2：新的location或新的月份
                    records_to_upsert.append(record)
                    if any(k[0] == key[0] for k in existing_records):
                        logger.info(f"Adding new month {record['year_month']} for location {record['location_name']}")
                        stats['new_months'] += 1
                    else:
                        logger.info(f"Adding new location: {record['location_name']}")
                        stats['new_locations'] += 1
                else:
                    # 情况3：更新现有记录
                    existing = existing_records[key]
                    merged_record = existing.copy()
                    merged_record.update({
                        'last_update_time': current_time.isoformat()
                    })
                    
                    # 检查每个租金字段
                    has_updates = False
                    for field in ['rent_estimate_overall', 'rent_estimate_1br', 'rent_estimate_2br']:
                        new_value = record.get(field)
                        if new_value is not None and new_value != 0:  # 新值不为空且不为0
                            merged_record[field] = new_value
                            has_updates = True
                            logger.debug(f"Updating {field} for {record['location_name']} {record['year_month']}: {existing.get(field)} -> {new_value}")
                    
                    if has_updates:
                        records_to_upsert.append(merged_record)
                        stats['updated'] += 1
                        logger.info(f"Updating existing record for {record['location_name']} {record['year_month']}")
                    else:
                        stats['preserved'] += 1
                        logger.debug(f"Preserving existing record for {record['location_name']} {record['year_month']}")
                        
            # 执行批量更新
            if records_to_upsert:
                result = self.client.table('apartment_list_rent_estimates').upsert(
                    records_to_upsert,
                    on_conflict='location_fips_code,year_month'
                ).execute()
                
                logger.info(
                    f"Update statistics:\n"
                    f"- New locations: {stats['new_locations']}\n"
                    f"- New months for existing locations: {stats['new_months']}\n"
                    f"- Updated records: {stats['updated']}\n"
                    f"- Preserved records: {stats['preserved']}"
                )
                
                return len(records_to_upsert)
            else:
                logger.info("No records need to be updated")
                return 0
                
        except Exception as e:
            raise DatabaseError(f"Failed to insert rent estimates: {str(e)}") from e
            
    def check_table_exists(self):
        """Check if the rent estimates table exists."""
        try:
            # 使用 REST API 查询表
            response = requests.get(
                f"{self.url}/rest/v1/apartment_list_rent_estimates?select=location_fips_code&limit=1",
                headers=self.headers
            )
            
            if response.status_code == 200:
                print("Table exists!")
                return True
            elif response.status_code == 404:
                print("Table does not exist. Please create it using Supabase's SQL editor with the correct schema.")
                print("Required schema:")
                print("- location_name: TEXT NOT NULL")
                print("- location_type: TEXT NOT NULL")
                print("- location_fips_code: TEXT NOT NULL")
                print("- population: INTEGER")
                print("- state: TEXT")
                print("- county: TEXT")
                print("- metro: TEXT")
                print("- year_month: TEXT NOT NULL (format: YYYY_MM)")
                print("- rent_estimate_overall: DOUBLE PRECISION")
                print("- rent_estimate_1br: DOUBLE PRECISION")
                print("- rent_estimate_2br: DOUBLE PRECISION")
                print("- PRIMARY KEY (location_fips_code, year_month)")
                return False
            else:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
            
        except Exception as e:
            raise DatabaseError(f"Failed to check rent estimates table: {str(e)}") from e

    def get_latest_year_month(self) -> Optional[str]:
        """Get the latest year_month from the rent estimates table."""
        try:
            response = requests.get(
                f"{self.url}/rest/v1/apartment_list_rent_estimates?select=year_month&order=year_month.desc&limit=1",
                headers=self.headers
            )
            
            if response.status_code == 200:
                data = response.json()
                if data and len(data) > 0:
                    return data[0]['year_month']
            return None
            
        except Exception as e:
            raise DatabaseError(f"Failed to get latest year_month: {str(e)}") from e
            
    def get_location_fips_codes(self) -> List[str]:
        """
        Get all unique location_fips_codes in the database.
        
        Returns:
            List[str]: List of unique location_fips_codes
            
        Raises:
            DatabaseError: If query fails
        """
        try:
            result = self.client.table('apartment_list_rent_estimates')\
                .select('location_fips_code')\
                .execute()
                
            return list(set(r['location_fips_code'] for r in result.data))
            
        except Exception as e:
            raise DatabaseError(f"Failed to get location_fips_codes: {str(e)}") from e

    def add_last_update_time_column(self) -> None:
        """
        Add last_update_time column to the rent estimates table if it doesn't exist.
        
        Raises:
            DatabaseError: If column addition fails
        """
        try:
            # Check if column exists
            sql = """
            DO $$ 
            BEGIN 
                IF NOT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name = 'apartment_list_rent_estimates' 
                    AND column_name = 'last_update_time'
                ) THEN 
                    ALTER TABLE apartment_list_rent_estimates 
                    ADD COLUMN last_update_time TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York');
                    
                    -- Update existing rows with WHERE clause
                    UPDATE apartment_list_rent_estimates 
                    SET last_update_time = (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')
                    WHERE last_update_time IS NULL;
                END IF;
            END $$;
            """
            
            self.execute_sql(sql)
            logger.info("Last update time column added or already exists")
            
        except Exception as e:
            raise DatabaseError(f"Failed to add last_update_time column: {str(e)}") from e

    def verify_timezone(self) -> None:
        """Verify the timezone settings for last_update_time."""
        try:
            # 使用 REST API 查询
            response = requests.get(
                f"{self.url}/rest/v1/apartment_list_rent_estimates",
                headers=self.headers,
                params={
                    'select': 'last_update_time',
                    'order': 'last_update_time.desc',
                    'limit': '5'
                }
            )
            
            if response.status_code >= 400:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
                
            data = response.json()
            if not data:
                print("No data found with timestamps")
                return
                
            print("\nTimezone verification results:")
            print("--------------------------------")
            for row in data:
                utc_time = row['last_update_time']
                # 从UTC时间中提取时区信息
                if '+00:00' in utc_time:
                    print(f"Time is stored in UTC format as expected")
                else:
                    print(f"Warning: Time is not stored in UTC format")
                print(f"Timestamp: {utc_time}")
                print("--------------------------------")
            
            print("\nNote: Times are stored in UTC format (+00:00)")
            print("When displayed in the application, they will be")
            print("automatically converted to US Eastern Time.")
            
        except Exception as e:
            raise DatabaseError(f"Failed to verify timezone: {str(e)}") from e 