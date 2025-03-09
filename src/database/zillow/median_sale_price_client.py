"""
Zillow Median Sale Price Database Client

This module handles database operations for Zillow median sale price data.
It includes functionality for inserting and updating data in Supabase.
"""

import logging
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from postgrest.exceptions import APIError

from ..base.base_client import BaseSupabaseClient
from ...utils.exceptions import DatabaseError

logger = logging.getLogger(__name__)

class MedianSalePriceClient(BaseSupabaseClient):
    """处理Zillow房屋销售中位数价格数据的数据库客户端"""
    
    TABLE_NAME = "zillow_median_sale_price_all_home"
    VIEW_NAME = "zillow_median_sale_price_all_home_view"
    
    def get_latest_date(self) -> Optional[str]:
        """
        Get the latest date from the database.
        
        Returns:
            Optional[str]: Latest date in format 'YYYY-MM-DD' or None if no data
        """
        try:
            result = self.execute_sql(
                f"""
                SELECT MAX(date) as latest_date 
                FROM {self.TABLE_NAME};
                """
            )
            return result.get("latest_date")
            
        except Exception as e:
            logger.error(f"Failed to get latest date: {e}")
            return None
    
    def get_unique_region_ids(self) -> List[str]:
        """
        Get list of unique region IDs.
        
        Returns:
            List[str]: List of unique region IDs
        """
        try:
            result = self.execute_sql(
                f"""
                SELECT DISTINCT region_id 
                FROM {self.TABLE_NAME} 
                ORDER BY region_id;
                """
            )
            return [row["region_id"] for row in result]
            
        except Exception as e:
            logger.error(f"Failed to get unique region IDs: {e}")
            return []
    
    def insert_records(self, records: List[Dict[str, Any]]) -> int:
        """
        Insert median sale price records into database.
        
        Args:
            records: List of median sale price records to insert
            
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
                    {processed_record['region_id']}, 
                    {processed_record['size_rank']}, 
                    {processed_record['region_name']},
                    {processed_record['region_type']}, 
                    {processed_record['state_name']}, 
                    {processed_record['date']},
                    {processed_record['median_sale_price_all_home']}
                )"""
                values_list.append(values)
            
            # 构建批量插入SQL
            query = f"""
                INSERT INTO {self.TABLE_NAME} (
                    region_id, size_rank, region_name,
                    region_type, state_name, date,
                    median_sale_price_all_home
                ) VALUES 
                {','.join(values_list)}
                ON CONFLICT (region_id, date) 
                DO UPDATE SET
                    median_sale_price_all_home = CASE 
                        WHEN EXCLUDED.median_sale_price_all_home IS NOT NULL 
                        THEN EXCLUDED.median_sale_price_all_home 
                        ELSE {self.TABLE_NAME}.median_sale_price_all_home 
                    END
            """
            
            # 执行批量插入
            result = self.execute_sql(query)
            
            # 刷新物化视图
            self.refresh_materialized_view(self.VIEW_NAME)
            
            processed_count = len(records)
            logger.info(f"Successfully inserted {processed_count} median sale price records")
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Failed to insert median sale price records: {e}")
            raise DatabaseError(f"Failed to insert median sale price records: {e}") from e 