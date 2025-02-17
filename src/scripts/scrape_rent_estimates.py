"""Script to scrape rent estimates data from Apartment List."""

import logging
import sys
from pathlib import Path

from ..scrapers.apartment_list.rent_estimates_scraper import RentEstimatesScraper
from ..utils.config import Config
from ..utils.exceptions import ConfigurationError, DataValidationError, ScrapingError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(Path("logs") / "rent_estimates_scraper.log")
    ]
)

logger = logging.getLogger(__name__)

def main():
    """Main entry point for the rent estimates scraping script."""
    try:
        # Load configuration
        config = Config.from_env(".env")
        
        # Initialize and run scraper
        scraper = RentEstimatesScraper(config)
        output_path = scraper.scrape()
        
        logger.info(f"Successfully scraped rent estimates data to: {output_path}")
        return 0
        
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except ScrapingError as e:
        logger.error(f"Scraping error: {e}")
        return 1
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        return 1
    except Exception as e:
        logger.exception("Unexpected error occurred")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 