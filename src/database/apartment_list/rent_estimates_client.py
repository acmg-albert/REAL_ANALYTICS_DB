"""Rent estimates Supabase client for database operations."""

import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from ..base.base_client import BaseSupabaseClient
from ...utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class RentEstimatesClient(BaseSupabaseClient):
    """Client for handling rent estimates data in Supabase."""
    
    TABLE_NAME = "apartment_list_rent_estimates"
    VIEW_NAME = "apartment_list_rent_estimates_view"
    
    def get_latest_year_month(self) -> Optional[str]:
        """
        Get the latest year_month from the database.
        
        Returns:
            Optional[str]: Latest year_month in format 'YYYY_MM' or None if no data
        """
        try:
            result = self.execute_sql(
                f"""
                SELECT MAX(year || '_' || LPAD(month::text, 2, '0')) as latest_year_month 
                FROM {self.TABLE_NAME};
                """
            )
            return result.get("latest_year_month")
            
        except Exception as e:
            logger.error(f"Failed to get latest year_month: {e}")
            return None
    
    def get_unique_location_fips(self) -> List[str]:
        """
        Get list of unique location FIPS codes.
        
        Returns:
            List[str]: List of unique FIPS codes
        """
        try:
            result = self.execute_sql(
                f"""
                SELECT DISTINCT location_fips_code 
                FROM {self.TABLE_NAME} 
                ORDER BY location_fips_code;
                """
            )
            return [row["location_fips_code"] for row in result]
            
        except Exception as e:
            logger.error(f"Failed to get unique FIPS codes: {e}")
            return []
    
    def insert_records(self, records: List[Dict[str, Any]]) -> int:
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
                processed_record = {
                    key: self.process_record_value(value)
                    for key, value in record.items()
                }
                
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
                INSERT INTO {self.TABLE_NAME} (
                    location_name, location_type, location_fips_code,
                    population, state, county, metro, year_month,
                    rent_estimate_overall, rent_estimate_1br, rent_estimate_2br
                ) VALUES 
                {','.join(values_list)}
                ON CONFLICT (location_fips_code, year_month) 
                DO UPDATE SET
                    rent_estimate_overall = CASE 
                        WHEN EXCLUDED.rent_estimate_overall IS NOT NULL THEN EXCLUDED.rent_estimate_overall 
                        ELSE {self.TABLE_NAME}.rent_estimate_overall 
                    END,
                    rent_estimate_1br = CASE 
                        WHEN EXCLUDED.rent_estimate_1br IS NOT NULL THEN EXCLUDED.rent_estimate_1br 
                        ELSE {self.TABLE_NAME}.rent_estimate_1br 
                    END,
                    rent_estimate_2br = CASE 
                        WHEN EXCLUDED.rent_estimate_2br IS NOT NULL THEN EXCLUDED.rent_estimate_2br 
                        ELSE {self.TABLE_NAME}.rent_estimate_2br 
                    END
            """
            
            # 执行批量插入
            result = self.execute_sql(query)
            
            # 刷新物化视图
            self.refresh_materialized_view(self.VIEW_NAME)
            
            processed_count = result.get('affected_rows', 0)
            logger.info(f"Successfully inserted {processed_count} rent estimate records")
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Failed to insert rent estimate records: {e}")
            raise DatabaseError(f"Failed to insert rent estimate records: {e}") from e 