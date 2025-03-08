from typing import Dict, List, Any
import logging
from ..base.base_client import BaseSupabaseClient, DatabaseError

logger = logging.getLogger(__name__)

class TimeOnMarketClient(BaseSupabaseClient):
    """Client for handling ApartmentList time on market data in Supabase."""
    
    def insert_records(self, records: List[Dict[str, Any]]) -> int:
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
                    {processed_record['time_on_market']}
                )"""
                values_list.append(values)
            
            # 构建批量插入SQL
            query = f"""
                INSERT INTO apartment_list_time_on_market (
                    location_name, location_type, location_fips_code,
                    population, state, county, metro, year_month,
                    time_on_market
                ) VALUES 
                {','.join(values_list)}
                ON CONFLICT (location_fips_code, year_month) 
                DO UPDATE SET
                    time_on_market = CASE 
                        WHEN EXCLUDED.time_on_market IS NOT NULL THEN EXCLUDED.time_on_market 
                        ELSE apartment_list_time_on_market.time_on_market 
                    END
            """
            
            # 执行批量插入
            self.execute_sql(query)
            
            # 刷新物化视图
            self.refresh_materialized_view('apartment_list_time_on_market_view')
            logger.info(f"成功批量插入 {len(records)} 条上市时间记录")
            
            return len(records)
            
        except Exception as e:
            logger.error(f"批量插入上市时间记录失败: {e}")
            raise DatabaseError(f"批量插入上市时间记录失败: {e}") from e 