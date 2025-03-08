"""Script to process ApartmentList vacancy index data."""

import logging
import sys
from pathlib import Path

from ..scrapers.apartment_list.vacancy_index_processor import VacancyIndexProcessor
from ..utils.config import Config
from ..utils.exceptions import (
    ConfigurationError,
    DataValidationError,
    ProcessingError,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def find_latest_raw_file() -> Path:
    """Find the latest raw vacancy index file in the data directory."""
    data_dir = Path("data")
    # 只查找原始文件，不包含 "processed" 的文件
    raw_files = [f for f in data_dir.glob("vacancy_index_2*.csv") 
                if "processed" not in f.name]
    
    if not raw_files:
        raise FileNotFoundError("No raw vacancy index files found in data directory")
    return max(raw_files, key=lambda x: x.stat().st_mtime)

def main():
    """Main function to process vacancy index data."""
    try:
        input_file = find_latest_raw_file()
        logger.info(f"Processing file: {input_file}")

        processor = VacancyIndexProcessor(input_file=input_file)
        output_path = processor.process()
        logger.info(f"Successfully processed data and saved to: {output_path}")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        sys.exit(1)
    except ProcessingError as e:
        logger.error(f"Processing error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 