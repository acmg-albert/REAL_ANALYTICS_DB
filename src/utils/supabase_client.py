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
        self.client: Client = create_client(url, key)
        logger.debug("Initialized Supabase client")
    
    def execute_sql(self, sql: str) -> Dict:
        """Execute raw SQL query.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Dict containing the query result
        """
        result = self.client.rpc('raw_sql', {'query': sql}).execute()
        return result.data
    
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
        """Insert vacancy index records into Supabase.
        
        Args:
            records: List of vacancy index records to insert
            
        Returns:
            Dict containing the insert operation result
        """
        result = self.client.table('apartment_list_vacancy_index').insert(records).execute()
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