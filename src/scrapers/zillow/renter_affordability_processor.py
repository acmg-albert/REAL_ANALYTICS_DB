"""
Zillow Renter Affordability Processor

This module handles the processing of new renter affordability data from Zillow.
It includes functionality for data cleaning, validation, and transformation.
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Tuple, Optional

from ...utils.exceptions import ProcessingError, DataValidationError

logger = logging.getLogger(__name__)

class RenterAffordabilityProcessor:
    """处理Zillow租房者可负担能力数据的处理器类"""
    
    def __init__(self):
        """初始化处理器"""
        self.logger = logging.getLogger(__name__)
        
    def _validate_raw_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """
        验证原始数据的格式和内容。
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            Tuple[bool, Optional[str]]: (是否有效, 错误信息)
        """
        try:
            # 检查必需的列
            required_cols = ['RegionID', 'SizeRank', 'RegionName', 'RegionType']
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                return False, f"缺少必需的列: {missing_cols}"
            
            # 检查数据类型
            if not pd.api.types.is_numeric_dtype(df['RegionID']):
                return False, "RegionID 必须是数字类型"
            if not pd.api.types.is_numeric_dtype(df['SizeRank']):
                return False, "SizeRank 必须是数字类型"
                
            # 检查日期列
            date_cols = [col for col in df.columns if col not in required_cols + ['StateName']]
            if not date_cols:
                return False, "没有找到日期列"
                
            # 验证日期列格式（支持两种格式：'YYYY-MM-DD' 和 'MM/DD/YYYY'）
            for col in date_cols:
                try:
                    # 尝试 'YYYY-MM-DD' 格式
                    datetime.strptime(col, '%Y-%m-%d')
                except ValueError:
                    try:
                        # 尝试 'MM/DD/YYYY' 格式
                        datetime.strptime(col, '%m/%d/%Y')
                    except ValueError:
                        return False, f"无效的日期列名格式: {col}"
                    
            # 检查数值
            for col in date_cols:
                if not pd.api.types.is_numeric_dtype(df[col].dropna()):
                    return False, f"列 {col} 包含非数值数据"
                    
            return True, None
            
        except Exception as e:
            return False, f"数据验证失败: {str(e)}"
            
    def _transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        将数据从宽格式转换为长格式，并进行必要的清理。
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            pd.DataFrame: 处理后的数据
        """
        try:
            # 获取元数据列和日期列
            metadata_cols = ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']
            date_cols = [col for col in df.columns if col not in metadata_cols]
            
            # 转换为长格式
            df_long = pd.melt(
                df,
                id_vars=metadata_cols,
                value_vars=date_cols,
                var_name='date',
                value_name='new_renter_affordability'
            )
            
            # 转换日期格式
            df_long['date'] = pd.to_datetime(df_long['date']).dt.strftime('%Y-%m-%d')
            
            # 重命名列
            column_mapping = {
                'RegionID': 'region_id',
                'SizeRank': 'size_rank',
                'RegionName': 'region_name',
                'RegionType': 'region_type',
                'StateName': 'state_name'
            }
            processed_df = df_long.rename(columns=column_mapping)
            
            # 处理空值
            processed_df['new_renter_affordability'] = processed_df['new_renter_affordability'].replace(
                [np.inf, -np.inf, np.nan], None
            )
            
            # 确保正确的数据类型
            processed_df['region_id'] = processed_df['region_id'].astype(str)
            processed_df['size_rank'] = processed_df['size_rank'].astype(int)
            processed_df['region_name'] = processed_df['region_name'].astype(str)
            processed_df['region_type'] = processed_df['region_type'].astype(str)
            processed_df['state_name'] = processed_df['state_name'].fillna('').astype(str)
            
            # 按日期和地区排序
            processed_df = processed_df.sort_values(['date', 'size_rank'])
            
            return processed_df
            
        except Exception as e:
            raise ProcessingError(f"数据转换失败: {str(e)}")
            
    def _validate_processed_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """
        验证处理后的数据。
        
        Args:
            df: 处理后的DataFrame
            
        Returns:
            Tuple[bool, Optional[str]]: (是否有效, 错误信息)
        """
        try:
            # 检查列数
            expected_cols = ['region_id', 'size_rank', 'region_name', 'region_type', 
                           'state_name', 'date', 'new_renter_affordability']
            if list(df.columns) != expected_cols:
                return False, f"列不匹配。期望: {expected_cols}, 实际: {list(df.columns)}"
            
            # 检查必需列的空值
            required_non_null = ['region_id', 'size_rank', 'region_name', 'region_type', 'date']
            for col in required_non_null:
                if df[col].isnull().any():
                    return False, f"列 {col} 包含空值"
            
            # 检查数据类型
            if not pd.api.types.is_integer_dtype(df['size_rank']):
                return False, "size_rank 必须是整数类型"
            if not pd.api.types.is_string_dtype(df['region_id']):
                return False, "region_id 必须是字符串类型"
            
            # 验证日期格式
            try:
                pd.to_datetime(df['date'])
            except ValueError:
                return False, "date 列包含无效的日期格式"
            
            return True, None
            
        except Exception as e:
            return False, f"数据验证失败: {str(e)}"
            
    def process(self, input_file: Path) -> Path:
        """
        处理Zillow租房者可负担能力数据。
        
        Args:
            input_file: 输入文件路径
            
        Returns:
            Path: 处理后的文件路径
            
        Raises:
            ProcessingError: 当处理过程失败时抛出
            DataValidationError: 当数据验证失败时抛出
        """
        try:
            self.logger.info(f"开始处理文件: {input_file}")
            
            # 读取数据
            df = pd.read_csv(input_file)
            self.logger.info(f"成功读取 {len(df)} 行数据")
            
            # 验证原始数据
            is_valid, error_msg = self._validate_raw_data(df)
            if not is_valid:
                raise DataValidationError(f"原始数据验证失败: {error_msg}")
            
            # 转换数据
            self.logger.info("开始转换数据")
            processed_df = self._transform_data(df)
            self.logger.info(f"成功转换 {len(processed_df)} 条记录")
            
            # 验证处理后的数据
            is_valid, error_msg = self._validate_processed_data(processed_df)
            if not is_valid:
                raise DataValidationError(f"处理后的数据验证失败: {error_msg}")
            
            # 保存处理后的数据
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = Path("data") / f"processed_zillow_renter_affordability_{timestamp}.csv"
            processed_df.to_csv(output_path, index=False)
            
            self.logger.info(f"数据处理完成，保存到: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"处理过程失败: {str(e)}")
            raise ProcessingError(f"处理过程失败: {str(e)}") from e 