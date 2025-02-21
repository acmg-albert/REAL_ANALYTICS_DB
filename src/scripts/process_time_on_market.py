"""Script to process time on market data."""

import logging
import sys
from pathlib import Path

from ..scrapers.apartment_list.time_on_market_processor import TimeOnMarketProcessor
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
    """Main entry point for the time on market processing script."""
    try:
        # Get the most recent raw data file
        data_dir = Path("data")
        csv_files = list(data_dir.glob("time_on_market_2*.csv"))
        if not csv_files:
            logger.error("No time on market data files found")
            return 1
            
        latest_file = max(csv_files, key=lambda p: p.stat().st_mtime)
        logger.info(f"Processing {latest_file}")
        
        # Process the data
        processor = TimeOnMarketProcessor(latest_file)
        output_path = processor.process()
        
        logger.info(f"Successfully processed time on market data to: {output_path}")
        return 0
        
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 