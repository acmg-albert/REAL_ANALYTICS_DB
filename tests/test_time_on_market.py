"""Tests for time on market data scraping and importing."""

import logging
import os
import sys
from pathlib import Path
from unittest import TestCase, main

import pandas as pd
from dotenv import load_dotenv

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.scrapers.apartment_list.time_on_market_scraper import TimeOnMarketScraper
from src.scripts.import_apartment_list_time_on_market import find_latest_processed_file, transform_data, validate_data, import_data_in_batches
from src.utils.config import Config
from src.utils.exceptions import DataValidationError, ScrapingError
from src.database import TimeOnMarketClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class TestTimeOnMarket(TestCase):
    """Test cases for time on market functionality."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Load environment variables
        load_dotenv()
        
        # Initialize configuration
        cls.config = Config.from_env()
        
        # Initialize Supabase client
        cls.client = TimeOnMarketClient(
            url=cls.config.supabase_url,
            key=cls.config.supabase_service_role_key
        )
        
        # Create test data directory
        cls.test_data_dir = Path("tests/data")
        cls.test_data_dir.mkdir(parents=True, exist_ok=True)
        
    def setUp(self):
        """Set up test case."""
        self.scraper = TimeOnMarketScraper(self.config)
        
    def test_scraping(self):
        """Test time on market data scraping."""
        try:
            # Run scraper
            output_path = self.scraper.scrape()
            
            # Verify file exists
            self.assertTrue(output_path.exists())
            
            # Verify file is not empty
            self.assertGreater(output_path.stat().st_size, 0)
            
            # Verify file is readable as CSV
            df = pd.read_csv(output_path)
            self.assertGreater(len(df), 0)
            
            # Verify required columns exist
            required_cols = [
                'location_name', 'location_type', 'location_fips_code',
                'population', 'state', 'county', 'metro'
            ]
            for col in required_cols:
                self.assertIn(col, df.columns)
                
            # Verify time series columns exist (YYYY_MM format)
            time_cols = [col for col in df.columns if col not in required_cols]
            self.assertGreater(len(time_cols), 0)
            
            logger.info("Scraping test passed")
            
        except ScrapingError as e:
            self.fail(f"Scraping failed: {str(e)}")
            
    def test_importing(self):
        """Test time on market data importing."""
        try:
            # Create test data
            test_data = {
                'location_name': ['Test City 1', 'Test City 2'],
                'location_type': ['City', 'City'],
                'location_fips_code': ['12345', '67890'],
                'population': [100000, 200000],
                'state': ['CA', 'NY'],
                'county': ['Test County 1', 'Test County 2'],
                'metro': ['Test Metro 1', 'Test Metro 2'],
                'year_month': ['2024_01', '2024_02'],
                'time_on_market': [30.5, 25.7]
            }
            
            test_df = pd.DataFrame(test_data)
            test_file = self.test_data_dir / "test_time_on_market.csv"
            test_df.to_csv(test_file, index=False)
            
            # Transform and validate data
            df_transformed = transform_data(test_df)
            validate_data(df_transformed)
            
            # Import data
            total_imported = import_data_in_batches(df_transformed, self.client)
            self.assertGreater(total_imported, 0)
            
            # Verify data in database
            result = self.client.execute_sql(
                f"""
                SELECT * FROM {self.client.TABLE_NAME}
                WHERE location_fips_code IN ('12345', '67890')
                ORDER BY location_fips_code;
                """
            )
            self.assertIsNotNone(result)
            self.assertGreater(len(result), 0)
            
            # Clean up test data
            test_file.unlink()
            
            # Clean up database test data
            self.client.execute_sql(
                f"""
                DELETE FROM {self.client.TABLE_NAME}
                WHERE location_fips_code IN ('12345', '67890');
                """
            )
            
            # Refresh materialized view
            self.client.refresh_materialized_view(self.client.VIEW_NAME)
            
            logger.info("Import test passed")
            
        except (DataValidationError, Exception) as e:
            self.fail(f"Import failed: {str(e)}")
            
    def test_end_to_end(self):
        """Test complete scraping and importing workflow."""
        try:
            # Run scraper
            output_path = self.scraper.scrape()
            
            # Read and transform data
            df = pd.read_csv(output_path)
            df_transformed = transform_data(df)
            validate_data(df_transformed)
            
            # Import data
            total_imported = import_data_in_batches(df_transformed, self.client)
            self.assertGreater(total_imported, 0)
            
            logger.info("End-to-end test passed")
            
        except Exception as e:
            self.fail(f"End-to-end test failed: {str(e)}")
            
if __name__ == '__main__':
    main() 