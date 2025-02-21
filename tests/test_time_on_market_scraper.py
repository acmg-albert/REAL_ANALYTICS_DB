"""Tests for time on market data scraping."""

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
from src.utils.config import Config
from src.utils.exceptions import ScrapingError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class TestTimeOnMarketScraper(TestCase):
    """Test cases for time on market scraper."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Load environment variables
        load_dotenv()
        
        # Initialize configuration
        cls.config = Config.from_env()
        
        # Create test data directory
        cls.test_data_dir = Path("tests/data")
        cls.test_data_dir.mkdir(parents=True, exist_ok=True)
        
        # Create logs directory
        logs_dir = Path("logs")
        logs_dir.mkdir(exist_ok=True)
        
    def setUp(self):
        """Set up test case."""
        self.scraper = TimeOnMarketScraper(self.config)
        
    def test_get_page_source(self):
        """Test getting page source."""
        try:
            page_source = self.scraper._get_page_source()
            
            # 验证返回的页面源代码
            self.assertIsInstance(page_source, str)
            self.assertGreater(len(page_source), 0)
            
            # 验证是否包含预期的HTML结构
            self.assertIn("<html", page_source.lower())
            self.assertIn("</html>", page_source.lower())
            
            # 验证是否保存了调试文件
            debug_file = Path("logs") / "time_on_market_response.html"
            self.assertTrue(debug_file.exists())
            self.assertGreater(debug_file.stat().st_size, 0)
            
            logger.info("Page source test passed")
            
        except ScrapingError as e:
            self.fail(f"Failed to get page source: {str(e)}")
            
    def test_extract_csv_url(self):
        """Test extracting CSV URL from page source."""
        try:
            # 首先获取页面源代码
            page_source = self.scraper._get_page_source()
            
            # 提取CSV URL
            csv_url = self.scraper._extract_csv_url(page_source)
            
            # 验证URL格式
            self.assertIsInstance(csv_url, str)
            self.assertTrue(csv_url.startswith("http"))
            self.assertTrue(csv_url.endswith(".csv"))
            
            # 验证URL中包含预期的关键词
            self.assertIn("time", csv_url.lower())
            self.assertIn("market", csv_url.lower())
            
            logger.info("CSV URL extraction test passed")
            logger.info(f"Found CSV URL: {csv_url}")
            
        except ScrapingError as e:
            self.fail(f"Failed to extract CSV URL: {str(e)}")
            
    def test_download_csv(self):
        """Test downloading CSV file."""
        try:
            # 获取CSV URL
            page_source = self.scraper._get_page_source()
            csv_url = self.scraper._extract_csv_url(page_source)
            
            # 设置测试输出路径
            output_path = self.test_data_dir / "test_time_on_market.csv"
            
            # 下载CSV文件
            self.scraper._download_csv(csv_url, output_path)
            
            # 验证文件是否下载成功
            self.assertTrue(output_path.exists())
            self.assertGreater(output_path.stat().st_size, 0)
            
            # 验证文件是否可以作为CSV读取
            df = pd.read_csv(output_path)
            self.assertGreater(len(df), 0)
            
            logger.info("CSV download test passed")
            
        except ScrapingError as e:
            self.fail(f"Failed to download CSV: {str(e)}")
            
    def test_validate_data(self):
        """Test data validation."""
        try:
            # 创建测试数据
            test_data = {
                'location_name': [f'Test City {i}' for i in range(1, 101)],
                'location_type': ['city'] * 100,
                'location_fips_code': [str(i).zfill(5) for i in range(1, 101)],
                'population': [i * 10000 for i in range(1, 101)],
                'state': ['CA', 'NY'] * 50,
                'county': [f'Test County {i}' for i in range(1, 101)],
                'metro': [f'Test Metro {i}' for i in range(1, 101)],
            }
            
            # 添加从2019年1月到2025年1月的时间列
            for year in range(2019, 2026):
                for month in range(1, 13):
                    if year == 2025 and month > 1:  # 只到2025年1月
                        break
                    col = f'{year}_{str(month).zfill(2)}'
                    test_data[col] = [30.5 + i * 0.1 for i in range(100)]
            
            test_df = pd.DataFrame(test_data)
            
            # 测试有效数据
            is_valid, error_msg = self.scraper._validate_data(test_df)
            self.assertTrue(is_valid, f"Validation failed: {error_msg}")
            self.assertIsNone(error_msg)
            
            # 测试缺少必需列
            invalid_df = test_df.drop('location_name', axis=1)
            is_valid, error_msg = self.scraper._validate_data(invalid_df)
            self.assertFalse(is_valid)
            self.assertIn("Missing required column", error_msg)
            
            # 测试负值
            invalid_df = test_df.copy()
            invalid_df['2024_01'] = -1
            is_valid, error_msg = self.scraper._validate_data(invalid_df)
            self.assertFalse(is_valid)
            self.assertIn("negative", error_msg)
            
            logger.info("Data validation test passed")
            
        except Exception as e:
            self.fail(f"Data validation test failed: {str(e)}")
            
    def test_scrape(self):
        """Test complete scraping workflow."""
        try:
            # 运行完整的抓取流程
            output_path = self.scraper.scrape()
            
            # 验证输出文件
            self.assertTrue(output_path.exists())
            self.assertGreater(output_path.stat().st_size, 0)
            
            # 验证文件内容
            df = pd.read_csv(output_path)
            self.assertGreater(len(df), 0)
            
            # 验证必需的列
            required_cols = [
                'location_name', 'location_type', 'location_fips_code',
                'population', 'state', 'county', 'metro'
            ]
            for col in required_cols:
                self.assertIn(col, df.columns)
                
            # 验证时间序列列（YYYY_MM格式）
            time_cols = [col for col in df.columns if col not in required_cols]
            self.assertGreater(len(time_cols), 0)
            
            logger.info("Complete scraping workflow test passed")
            
        except Exception as e:
            self.fail(f"Complete scraping workflow test failed: {str(e)}")
            
if __name__ == '__main__':
    main() 