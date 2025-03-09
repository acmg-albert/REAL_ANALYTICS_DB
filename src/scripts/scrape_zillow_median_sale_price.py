"""
Script to download Zillow median sale price data.

This script uses the MedianSalePriceScraper to download and validate
median sale price data from Zillow.
"""

import logging
import sys
from pathlib import Path

from ..scrapers.zillow.median_sale_price_scraper import MedianSalePriceScraper
from ..utils.config import Config
from ..utils.exceptions import ScrapingError
from ..utils.logger import get_logger

logger = get_logger(__name__)

def main():
    """主函数"""
    try:
        # 加载配置
        config = Config.from_env()
        
        # 创建下载器
        scraper = MedianSalePriceScraper(config)
        
        # 下载数据
        output_path = scraper.scrape()
        
        logger.info(f"数据已成功下载到: {output_path}")
        return 0
        
    except ScrapingError as e:
        logger.error(f"下载数据失败: {str(e)}")
        return 1
    except Exception as e:
        logger.exception("发生未预期的错误")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 