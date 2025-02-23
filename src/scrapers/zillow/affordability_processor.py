"""
Zillow Affordability Processor

This module handles the processing of new homeowner affordability data from wide format to long format
and prepares it for database insertion.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from tqdm import tqdm

from ...utils.exceptions import DataValidationError

logger = logging.getLogger(__name__)

class AffordabilityProcessor:
    """Processor for Zillow new homeowner affordability data."""
    
    def __init__(self):
        """Initialize the processor."""
        pass
        
    def _read_data(self, input_file: Path) -> pd.DataFrame:
        """
        Read the input CSV file.
        
        Args:
            input_file: Path to the input CSV file
            
        Returns:
            pd.DataFrame: The raw data
        """
        logger.info(f"Reading data from {input_file}")
        return pd.read_csv(input_file)
        
    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        转换数据格式。
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 转换后的数据
        """
        # 复制基础列
        base_cols = ['RegionID', 'RegionName', 'RegionType', 'StateName']
        
        # 识别日期列（除了基础列之外的所有列）
        date_cols = [col for col in df.columns if col not in base_cols]
        
        # 将宽格式转换为长格式
        df_long = df.melt(
            id_vars=base_cols,
            value_vars=date_cols,
            var_name='Date',
            value_name='new_home_affordability_down_20pct'
        )
        
        # 转换日期格式
        df_long['Date'] = pd.to_datetime(df_long['Date'], format='%m/%d/%Y')
        
        # 重命名列以匹配数据库格式
        df_long = df_long.rename(columns={
            'RegionID': 'region_id',
            'RegionName': 'region_name',
            'RegionType': 'region_type',
            'StateName': 'state_name',
            'Date': 'date'
        })
        
        # 按region_id和date排序
        df_long = df_long.sort_values(['region_id', 'date'])
        
        return df_long
        
    def _validate_transformed_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """
        验证转换后的数据。
        
        Args:
            df: 转换后的数据
            
        Returns:
            Tuple[bool, Optional[str]]: (是否有效, 错误信息)
        """
        # 检查必需的列
        required_cols = [
            'region_id', 'region_name', 'region_type', 'state_name',
            'date', 'new_home_affordability_down_20pct'
        ]
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"缺少必需的列: {missing_cols}"
            
        # 检查空值
        for col in ['region_id', 'region_name', 'region_type', 'state_name', 'date']:
            if df[col].isna().any():
                return False, f"在必需列中发现空值: {col}"
                
        # 检查数据类型
        try:
            # 验证数值类型
            df['region_id'] = df['region_id'].astype(str)
            df['new_home_affordability_down_20pct'] = pd.to_numeric(df['new_home_affordability_down_20pct'])
            
            # 验证日期范围
            min_date = df['date'].min()
            max_date = df['date'].max()
            if min_date.year < 2012 or max_date.year > 2025:
                return False, f"日期范围无效: {min_date} 到 {max_date}"
                
        except (ValueError, TypeError) as e:
            return False, f"数据类型验证失败: {str(e)}"
            
        return True, None
        
    def process(self, input_file: Path) -> Path:
        """
        Process the affordability data.
        
        Args:
            input_file: Path to the input CSV file
            
        Returns:
            Path: Path to the processed CSV file
            
        Raises:
            DataValidationError: If data validation fails
        """
        # Read the data
        df = self._read_data(input_file)
        
        # Transform the data
        df_processed = self._transform_data(df)
        
        # Validate the transformed data
        is_valid, error_msg = self._validate_transformed_data(df_processed)
        if not is_valid:
            raise DataValidationError(f"Transformed data validation failed: {error_msg}")
            
        # Save the processed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = input_file.parent / f"zillow_affordability_processed_{timestamp}.csv"
        
        logger.info(f"Saving processed data to {output_path}")
        df_processed.to_csv(output_path, index=False)
        
        return output_path 