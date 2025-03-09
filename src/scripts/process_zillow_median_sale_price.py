"""
Script to process Zillow median sale price data.

This script processes the downloaded Zillow median sale price data,
transforming it from wide format to long format and preparing it for database import.
"""

import logging
import sys
from pathlib import Path

from ..scrapers.zillow.median_sale_price_processor import MedianSalePriceProcessor
from ..utils.exceptions import ProcessingError, DataValidationError
from ..utils.logger import get_logger

logger = get_logger(__name__)

def find_latest_raw_file(data_dir: Path) -> Path:
    """Find the latest raw Zillow median sale price file."""
    # 只查找原始文件，不包含 "processed" 的文件
    raw_files = [f for f in data_dir.glob("zillow_median_sale_price_2*.csv") 
                if "processed" not in f.name]
    
    if not raw_files:
        raise FileNotFoundError("No raw Zillow median sale price files found")
    latest_file = max(raw_files, key=lambda p: p.stat().st_mtime)
    logger.debug(f"Found latest file: {latest_file}")
    return latest_file

def main():
    """Main entry point for processing Zillow median sale price data."""
    try:
        # Find latest raw data file
        data_dir = Path("data")
        input_file = find_latest_raw_file(data_dir)
        logger.info(f"Processing file: {input_file}")
        
        # Initialize processor and process data
        processor = MedianSalePriceProcessor()
        output_path = processor.process(input_file)
        
        logger.info(f"Successfully processed data and saved to: {output_path}")
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        return 1
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        return 1
    except ProcessingError as e:
        logger.error(f"Processing error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == '__main__':
    sys.exit(main()) 