"""
Apartment List Rent Estimates Processor

This module handles the processing of rent estimates data from wide format to long format
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

class RentEstimatesProcessor:
    """Processor for Apartment List rent estimates data."""
    
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
        
        # Identify month columns (YYYY_MM format)
        month_cols = [col for col in df.columns if col.replace('_', '').isdigit() and len(col) == 7]
        
        # Melt the dataframe to convert months from columns to rows
        id_vars = ['location_name', 'location_type', 'location_fips_code', 
                  'population', 'state', 'county', 'metro', 'bed_size']
        
        df_long = pd.melt(
            df,
            id_vars=id_vars,
            value_vars=month_cols,
            var_name='year_month',
            value_name='rent_value'
        )
        
        return df_long
        
    def _pivot_bed_sizes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform bed_size from rows to columns.
        
        Args:
            df: Input dataframe with bed_size as rows
            
        Returns:
            pd.DataFrame: Transformed dataframe with bed_size as columns
        """
        logger.info("Pivoting bed sizes to columns")
        
        # Create separate columns for each bed_size
        df_pivot = df.pivot(
            index=['location_name', 'location_type', 'location_fips_code',
                   'population', 'state', 'county', 'metro', 'year_month'],
            columns='bed_size',
            values='rent_value'
        ).reset_index()
        
        # Rename columns to match database schema
        df_pivot.rename(columns={
            'overall': 'rent_estimate_overall',
            '1br': 'rent_estimate_1br',
            '2br': 'rent_estimate_2br'
        }, inplace=True)
        
        return df_pivot
        
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
            'rent_estimate_overall', 'rent_estimate_1br', 'rent_estimate_2br'
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
            
        # Verify rent values are numeric and non-negative
        rent_cols = ['rent_estimate_overall', 'rent_estimate_1br', 'rent_estimate_2br']
        for col in rent_cols:
            if (df[col] < 0).any():
                return False, f"Found negative rent values in column: {col}"
                
        return True, None
        
    def process(self) -> Path:
        """
        Process the rent estimates data.
        
        Returns:
            Path: Path to the processed CSV file
            
        Raises:
            DataValidationError: If data validation fails
        """
        # Read the data
        df = self._read_data()
        
        # Transform to long format
        df_long = self._transform_to_long_format(df)
        
        # Pivot bed sizes to columns
        df_processed = self._pivot_bed_sizes(df_long)
        
        # Validate the transformed data
        is_valid, error_msg = self._validate_transformed_data(df_processed)
        if not is_valid:
            raise DataValidationError(f"Transformed data validation failed: {error_msg}")
            
        # Save the processed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = self.input_file.parent / f"rent_estimates_processed_{timestamp}.csv"
        
        logger.info(f"Saving processed data to {output_path}")
        df_processed.to_csv(output_path, index=False)
        
        return output_path 