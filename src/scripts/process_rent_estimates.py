"""Script to process rent estimates data."""

import logging
import sys
from pathlib import Path

from ..scrapers.apartment_list.rent_estimates_processor import RentEstimatesProcessor
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
    """Main entry point for the rent estimates processing script."""
    try:
        # Get the most recent raw data file
        data_dir = Path("data")
        csv_files = list(data_dir.glob("rent_estimates_2*.csv"))
        if not csv_files:
            logger.error("No rent estimates data files found")
            return 1
            
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Processing {latest_file}")
        
        # Process the data
        processor = RentEstimatesProcessor(latest_file)
        output_path = processor.process()
        
        logger.info(f"Successfully processed rent estimates data to: {output_path}")
        return 0
        
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 