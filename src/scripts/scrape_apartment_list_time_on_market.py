"""Script to scrape ApartmentList time on market data."""

import logging
import sys
from pathlib import Path

from ..scrapers.apartment_list.time_on_market_scraper import TimeOnMarketScraper
from ..utils.config import Config
from ..utils.exceptions import ConfigurationError, DataValidationError, ScrapingError

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
    """Main entry point for the ApartmentList time on market scraping script."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize and run scraper
        scraper = TimeOnMarketScraper(config)
        output_path = scraper.scrape()
        
        logger.info(f"Successfully scraped time on market data to: {output_path}")
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