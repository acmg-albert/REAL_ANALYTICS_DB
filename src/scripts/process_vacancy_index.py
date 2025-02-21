"""Script to process vacancy index data."""

import logging
import sys
from pathlib import Path

from ..scrapers.apartment_list.vacancy_index_processor import VacancyIndexProcessor
from ..utils.exceptions import DataValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main entry point for the vacancy index processing script."""
    try:
        # Get the most recent raw data file
        data_dir = Path("data")
        csv_files = list(data_dir.glob("vacancy_index_2*.csv"))
        if not csv_files:
            logger.error("No vacancy index data files found")
            return 1
            
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Processing {latest_file}")
        
        # Process the data
        processor = VacancyIndexProcessor(latest_file)
        output_path = processor.process()
        
        logger.info(f"Successfully processed vacancy index data to: {output_path}")
        return 0
        
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 