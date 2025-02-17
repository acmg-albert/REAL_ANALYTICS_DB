"""Supabase database client module."""

import logging
from typing import Dict, List, Optional

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
            key: Supabase API key
        """
        self.client: Client = create_client(url, key)
        self.url = url.rstrip('/')
        self.key = key
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        
    def execute_sql(self, sql: str) -> None:
        """
        Execute raw SQL query.
        
        Args:
            sql: SQL query to execute
            
        Raises:
            DatabaseError: If query execution fails
        """
        try:
            # 使用 SQL 编辑器 API 端点
            response = requests.post(
                f"{self.url}/rest/v1/rpc/raw_sql",
                headers=self.headers,
                json={'command': sql}  # 使用 'command' 而不是 'sql'
            )
            
            if response.status_code >= 400:
                error_text = response.text if response.text else f"HTTP {response.status_code}"
                raise Exception(error_text)
                
        except Exception as e:
            raise DatabaseError(f"Failed to execute SQL: {str(e)}") from e
            
    def create_rent_estimates_table(self) -> None:
        """
        Create the rent estimates table if it doesn't exist.
        
        Raises:
            DatabaseError: If table creation fails
        """
        try:
            # Using raw SQL for table creation
            sql = """
            CREATE TABLE IF NOT EXISTS apartment_list_rent_estimates (
                location_name TEXT NOT NULL,
                location_type TEXT NOT NULL,
                location_fips_code TEXT NOT NULL,
                population BIGINT,
                state TEXT,
                county TEXT,
                metro TEXT,
                year_month TEXT NOT NULL,
                rent_estimate_overall DOUBLE PRECISION,
                rent_estimate_1br DOUBLE PRECISION,
                rent_estimate_2br DOUBLE PRECISION,
                last_update_time TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York'),
                PRIMARY KEY (location_fips_code, year_month)
            );
            """
            
            # Create the table using postgrest API
            headers = {
                'apikey': self.key,
                'Authorization': f'Bearer {self.key}',
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
            }
            
            # Create the table using direct SQL
            response = requests.post(
                f"{self.url}/rest/v1/rpc/raw_sql",
                headers=headers,
                json={'sql': sql}
            )
            
            if response.status_code >= 400:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
                
            logger.info("Rent estimates table created or already exists")
            
        except Exception as e:
            raise DatabaseError(f"Failed to create rent estimates table: {str(e)}") from e
            
    def insert_rent_estimates(self, records: List[Dict]) -> int:
        """
        Insert rent estimates records into the database.
        
        Args:
            records: List of rent estimates records
            
        Returns:
            int: Number of records inserted
            
        Raises:
            DatabaseError: If insertion fails
        """
        try:
            logger.info(f"Inserting {len(records)} records")
            
            # Add last_update_time to each record with ET timezone
            for record in records:
                record['last_update_time'] = "(CURRENT_TIMESTAMP AT TIME ZONE 'America/New_York')"
            
            result = self.client.table('apartment_list_rent_estimates').upsert(
                records,
                on_conflict='location_fips_code,year_month'
            ).execute()
            
            count = len(result.data)
            logger.info(f"Successfully inserted/updated {count} records")
            return count
            
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