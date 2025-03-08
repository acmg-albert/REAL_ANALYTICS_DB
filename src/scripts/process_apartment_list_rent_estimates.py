"""Script to process ApartmentList rent estimates data."""

import logging
import sys
from pathlib import Path

from ..scrapers.apartment_list.rent_estimates_processor import RentEstimatesProcessor
from ..utils.exceptions import ProcessingError, DataValidationError

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
    """Find the latest raw rent estimates file in the data directory."""
    data_dir = Path("data")
    # 只查找原始文件，不包含 "processed" 的文件
    raw_files = [f for f in data_dir.glob("rent_estimates_2*.csv") 
                if "processed" not in f.name]
    
    if not raw_files:
        raise FileNotFoundError("No raw rent estimates files found")
        
    return max(raw_files, key=lambda p: p.stat().st_mtime)

def main():
    """Main entry point for the ApartmentList rent estimates processing script."""
    try:
        # Find latest raw data file
        input_file = find_latest_raw_file()
        logger.info(f"Processing file: {input_file}")
        
        # Initialize processor with input file and process data
        processor = RentEstimatesProcessor(input_file)
        output_path = processor.process()
        
        logger.info(f"Successfully processed rent estimates data to: {output_path}")
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

if __name__ == "__main__":
    sys.exit(main()) 