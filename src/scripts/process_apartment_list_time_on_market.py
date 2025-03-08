"""Script to process ApartmentList time on market data."""

import logging
import sys
from pathlib import Path

from ..scrapers.apartment_list.time_on_market_processor import TimeOnMarketProcessor
from ..utils.exceptions import DataValidationError, ProcessingError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def find_latest_raw_file() -> Path:
    """Find the latest raw time on market file in the data directory."""
    data_dir = Path("data")
    # 只查找原始文件，不包含 "processed" 的文件
    raw_files = [f for f in data_dir.glob("time_on_market_2*.csv") 
                if "processed" not in f.name]
    
    if not raw_files:
        raise FileNotFoundError("No raw time on market files found in data directory")
        
    latest_file = max(raw_files, key=lambda x: x.stat().st_mtime)
    return latest_file

def main():
    """Main entry point for the ApartmentList time on market processing script."""
    try:
        # Find latest raw data file
        input_file = find_latest_raw_file()
        logger.info(f"Processing file: {input_file}")
        
        # Initialize processor and process data
        processor = TimeOnMarketProcessor(input_file=input_file)
        processor.process()
        
        logger.info(f"Successfully processed time on market data to: {input_file}")
        return 0
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        return 1
    except DataValidationError as e:
        logger.error(f"Data validation failed: {e}")
        return 1
    except ProcessingError as e:
        logger.error(f"Processing failed: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 