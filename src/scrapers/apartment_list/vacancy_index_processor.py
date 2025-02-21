"""
Apartment List Vacancy Index Processor

This module handles the processing of vacancy index data from wide format to long format
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

class VacancyIndexProcessor:
    """Processor for Apartment List vacancy index data."""
    
    def __init__(self, input_file: Path):
        """
        Initialize the processor.
        
        Args:
            input_file: Path to the input CSV file
        """
        self.input_file = input_file
        
    def _read_data(self) -> pd.DataFrame:
        """
        Read the input CSV file.
        
        Returns:
            pd.DataFrame: The raw data
        """
        logger.info(f"Reading data from {self.input_file}")
        return pd.read_csv(self.input_file)
        
    def _transform_to_long_format(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data from wide format (months as columns) to long format.
        
        Args:
            df: Input dataframe in wide format
            
        Returns:
            pd.DataFrame: Transformed dataframe in long format
        """
        logger.info("Transforming data to long format")
        
        # 确保location_fips_code是字符串类型
        df['location_fips_code'] = df['location_fips_code'].astype(str)
        
        # Identify month columns (YYYY_MM format)
        month_cols = [col for col in df.columns if col.replace('_', '').isdigit() and len(col) == 7]
        
        # Melt the dataframe to convert months from columns to rows
        id_vars = ['location_name', 'location_type', 'location_fips_code', 
                  'population', 'state', 'county', 'metro']
        
        df_long = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=month_cols,
            var_name='year_month',
            value_name='vacancy_index'
        )
        
        # 处理空值、NaN和异常值
        logger.info("Processing null, NaN and invalid values")
        
        # 将NaN值替换为None
        df_long = df_long.replace({float('nan'): None})
        
        # 将空值和异常值（小于0或大于1）替换为None
        df_long.loc[df_long['vacancy_index'].isna(), 'vacancy_index'] = None
        df_long.loc[(df_long['vacancy_index'] < 0) | (df_long['vacancy_index'] > 1), 'vacancy_index'] = None
        
        # 处理其他字段的空值
        df_long['population'] = df_long['population'].fillna(0).astype(int)
        df_long['state'] = df_long['state'].fillna('').astype(str)
        df_long['county'] = df_long['county'].fillna('').astype(str)
        df_long['metro'] = df_long['metro'].fillna('').astype(str)
        
        return df_long
        
    def _validate_transformed_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """
        Validate the transformed data.
        
        Args:
            df: Transformed dataframe
            
        Returns:
            Tuple[bool, Optional[str]]: (is_valid, error_message)
        """
        logger.info("Validating transformed data")
        
        # Check required columns
        required_cols = [
            'location_name', 'location_type', 'location_fips_code',
            'population', 'state', 'county', 'metro', 'year_month',
            'vacancy_index'
        ]
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"
            
        # Check for null values in required fields
        required_non_null = ['location_name', 'location_type', 'location_fips_code', 'year_month']
        for col in required_non_null:
            if df[col].isna().any():
                return False, f"Found null values in required column: {col}"
                
        # Verify year_month format
        invalid_dates = df[~df['year_month'].str.match(r'^\d{4}_\d{2}$')]
        if not invalid_dates.empty:
            return False, f"Found invalid year_month format: {invalid_dates['year_month'].unique()}"
            
        # 验证非空的vacancy_index值是否在有效范围内
        valid_values = df[df['vacancy_index'].notna()]
        if not valid_values.empty:
            invalid_values = valid_values[
                (valid_values['vacancy_index'] < 0) | 
                (valid_values['vacancy_index'] > 1)
            ]
            if not invalid_values.empty:
                return False, "Found invalid vacancy index values (not between 0 and 1)"
            
        return True, None
        
    def process(self) -> Path:
        """
        Process the vacancy index data.
        
        Returns:
            Path: Path to the processed CSV file
            
        Raises:
            DataValidationError: If data validation fails
        """
        # Read the data
        df = self._read_data()
        
        # Transform to long format
        df_long = self._transform_to_long_format(df)
        
        # Validate the transformed data
        is_valid, error_msg = self._validate_transformed_data(df_long)
        if not is_valid:
            raise DataValidationError(f"Transformed data validation failed: {error_msg}")
            
        # Save the processed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = self.input_file.parent / f"vacancy_index_processed_{timestamp}.csv"
        
        logger.info(f"Saving processed data to {output_path}")
        df_long.to_csv(output_path, index=False)
        
        return output_path 