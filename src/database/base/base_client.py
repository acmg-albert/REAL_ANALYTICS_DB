"""Base Supabase client for database operations."""

import logging
from typing import Any, Dict, List, Optional, Union
from supabase import Client, create_client

from ...utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class BaseSupabaseClient:
    """Base client for interacting with Supabase database."""
    
    def __init__(self, url: str, key: str) -> None:
        """
        Initialize base Supabase client.
        
        Args:
            url: Supabase project URL
            key: Supabase API key (anon or service role)
        """
        try:
            self.client = create_client(url, key)
            self.url = url
            self.key = key
            logger.info("Base Supabase client initialized successfully")
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
    
    def check_table_exists(self, table_name: str) -> bool:
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
    
    def refresh_materialized_view(self, view_name: str) -> None:
        """
        Refresh a specific materialized view.
        
        Args:
            view_name: Name of the materialized view to refresh
            
        Raises:
            DatabaseError: If refresh fails
        """
        try:
            self.client.rpc('raw_sql', {
                'command': f'REFRESH MATERIALIZED VIEW {view_name};'
            }).execute()
            logger.info(f"Successfully refreshed materialized view: {view_name}")
            
        except Exception as e:
            logger.error(f"Failed to refresh materialized view {view_name}: {e}")
            raise DatabaseError(f"Failed to refresh materialized view {view_name}: {e}") from e
    
    def process_record_value(self, value: Any) -> str:
        """
        Process a record value for SQL insertion.
        
        Args:
            value: Value to process
            
        Returns:
            str: Processed value ready for SQL insertion
        """
        if value is None:
            return 'NULL'
        elif isinstance(value, str):
            return f"'{value.replace(chr(39), chr(39)+chr(39))}'"
        else:
            return str(value) 