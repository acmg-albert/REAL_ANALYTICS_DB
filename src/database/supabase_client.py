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
        self.url = url
        self.key = key
        self.headers = {
            'apikey': self.key,
            'Authorization': f'Bearer {self.key}',
            'Content-Type': 'application/json',
            'Prefer': 'return=minimal'
        }
        
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