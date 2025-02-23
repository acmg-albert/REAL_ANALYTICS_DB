"""Script to process Zillow new renter affordability data."""

import logging
import sys
from pathlib import Path

from ..scrapers.zillow.renter_affordability_processor import RenterAffordabilityProcessor
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

def find_latest_raw_file(data_dir: Path) -> Path:
    """Find the latest raw Zillow renter affordability file."""
    raw_files = list(data_dir.glob("zillow_renter_affordability_*.csv"))
    if not raw_files:
        raise FileNotFoundError("No raw Zillow renter affordability files found")
    latest_file = max(raw_files, key=lambda p: p.stat().st_mtime)
    logger.debug(f"Found latest file: {latest_file}")
    return latest_file

def main():
    """Main entry point for processing Zillow renter affordability data."""
    try:
        # Find latest raw data file
        data_dir = Path("data")
        input_file = find_latest_raw_file(data_dir)
        logger.info(f"Processing file: {input_file}")
        
        # Initialize processor and process data
        processor = RenterAffordabilityProcessor()
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