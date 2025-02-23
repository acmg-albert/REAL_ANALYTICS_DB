"""Script to process Zillow new homeowner affordability data."""

import logging
import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os
from datetime import datetime
from src.utils.logger import get_logger
from src.utils.exceptions import ConfigurationError, ProcessingError, DataValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = get_logger(__name__)

def _validate_data(df):
    """验证数据格式和内容"""
    required_columns = ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']
    
    # 检查必需列是否存在
    for col in required_columns:
        if col not in df.columns:
            raise DataValidationError(f"缺少必需列: {col}")
    
    # 验证数据类型
    if not pd.api.types.is_numeric_dtype(df['RegionID']):
        raise DataValidationError("RegionID 必须是数字类型")
    if not pd.api.types.is_numeric_dtype(df['SizeRank']):
        raise DataValidationError("SizeRank 必须是数字类型")

def process_zillow_affordability():
    """处理 Zillow 可负担能力数据"""
    try:
        # 获取数据目录
        data_dir = os.path.join('data')
        if not os.path.exists(data_dir):
            raise ConfigurationError(f"数据目录不存在: {data_dir}")

        # 查找最新的数据文件
        files = [f for f in os.listdir(data_dir) if f.startswith('zillow_affordability_') and f.endswith('.csv')]
        if not files:
            raise ConfigurationError("未找到可负担能力数据文件")
        
        latest_file = max(files)
        file_path = os.path.join(data_dir, latest_file)
        logger.info(f"处理文件: {file_path}")

        # 读取数据
        df = pd.read_csv(file_path)
        
        # 验证数据
        _validate_data(df)
        
        # 将宽格式转换为长格式
        df_long = pd.melt(
            df,
            id_vars=['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName'],
            var_name='date',
            value_name='new_home_affordability_down_20pct'
        )

        # 转换日期格式
        df_long['date'] = pd.to_datetime(df_long['date'])
        
        # 重命名列以匹配数据库表结构
        df_long = df_long.rename(columns={
            'RegionID': 'region_id',
            'SizeRank': 'size_rank',
            'RegionName': 'region_name',
            'RegionType': 'region_type',
            'StateName': 'state_name'
        })
        
        # 处理NaN值 - 将无穷大和NaN值转换为None
        df_long['new_home_affordability_down_20pct'] = df_long['new_home_affordability_down_20pct'].replace([np.inf, -np.inf, np.nan], None)
        
        # 确保region_id是字符串类型
        df_long['region_id'] = df_long['region_id'].astype(str)
        
        # 按日期和地区排序
        df_long = df_long.sort_values(['date', 'size_rank'])
        
        # 保存处理后的数据
        output_file = os.path.join(data_dir, 'processed_' + latest_file)
        df_long.to_csv(output_file, index=False)
        logger.info(f"数据已保存到: {output_file}")
        logger.info(f"处理了 {len(df_long)} 条记录")
        logger.info(f"其中包含 {df_long['new_home_affordability_down_20pct'].isna().sum()} 条空值记录")

    except Exception as e:
        logger.error(f"处理数据时出错: {str(e)}")
        raise ProcessingError(f"处理数据失败: {str(e)}")

if __name__ == '__main__':
    process_zillow_affordability() 