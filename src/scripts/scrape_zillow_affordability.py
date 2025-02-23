"""Script to scrape Zillow new homeowner affordability data."""

import logging
import sys
from pathlib import Path

from ..scrapers.zillow import AffordabilityScraper
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
    """Main entry point for the Zillow affordability scraping script."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize and run scraper
        scraper = AffordabilityScraper(config)
        output_path = scraper.scrape()
        
        logger.info(f"Successfully scraped Zillow affordability data to: {output_path}")
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