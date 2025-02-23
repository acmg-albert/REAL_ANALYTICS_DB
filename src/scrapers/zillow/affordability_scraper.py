"""
Zillow Affordability Scraper

This module handles the scraping of new homeowner affordability data from Zillow.
It includes functionality for downloading CSV data and validating its contents.
"""

import logging
import re
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from tqdm import tqdm

from ...utils.config import Config
from ...utils.exceptions import DataValidationError, ScrapingError

logger = logging.getLogger(__name__)

class AffordabilityScraper:
    """负责从Zillow抓取新房主可负担能力数据的爬虫类"""
    
    BASE_URL = "https://www.zillow.com/research/data/"
    CSV_PATTERN = r'https:\\?/\\?/files\.zillowstatic\.com\\?/research\\?/public_csvs\\?/new_homeowner_affordability\\?/Metro_new_homeowner_affordability.*\.csv\?t=\d+'
    
    def __init__(self, config):
        """初始化爬虫
        
        Args:
            config: 包含配置信息的对象
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 确保必要的目录存在
        for path in ["logs", "data", "data/raw"]:
            Path(path).mkdir(exist_ok=True)
        
        self.session = requests.Session()
        # 随机选择一个User-Agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/120.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        self.session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
            'DNT': '1',
            'Referer': 'https://www.zillow.com/research/'
        })
        
    @retry(
        stop=stop_after_attempt(5),  # 增加重试次数
        wait=wait_exponential(multiplier=2, min=4, max=30),  # 增加等待时间
        before_sleep=before_sleep_log(logger, logging.INFO)  # 在重试前记录日志
    )
    def _get_page_source(self) -> str:
        """
        Get the page source with retry mechanism.
        
        Returns:
            str: The page source HTML
        
        Raises:
            ScrapingError: If unable to fetch the page after retries
        """
        try:
            # 添加随机延迟
            delay = random.uniform(2, 5)
            self.logger.info(f"等待 {delay:.2f} 秒后发起请求...")
            time.sleep(delay)
            
            # 首先访问研究页面
            self.logger.info(f"正在访问页面: {self.BASE_URL}")
            
            response = self.session.get(
                self.BASE_URL,
                timeout=self.config.request_timeout,
                allow_redirects=True
            )
            self.logger.info(f"获得响应: 状态码={response.status_code}")
            
            # 无论成功与否都保存响应内容
            debug_file = Path("logs") / "zillow_response.html"
            debug_file.parent.mkdir(exist_ok=True)
            with open(debug_file, "w", encoding="utf-8") as f:
                content = response.text
                f.write(content)
                self.logger.info(f"响应内容已保存到: {debug_file} (大小: {len(content)} 字节)")
            
            # 检查响应状态
            response.raise_for_status()
            
            # 检查响应内容
            if len(response.text.strip()) == 0:
                raise ScrapingError("页面响应内容为空")
                
            # 检查是否包含验证页面
            if 'px-captcha' in response.text or 'Access to this page has been denied' in response.text:
                raise ScrapingError("遇到验证页面，需要重试")
            
            return response.text
            
        except requests.RequestException as e:
            self.logger.error(f"获取页面失败: {str(e)}")
            if hasattr(e, 'response') and e.response is not None and hasattr(e.response, 'text'):
                self.logger.error(f"错误响应内容: {e.response.text[:500]}...")
            raise ScrapingError(f"获取页面失败: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"发生意外错误: {str(e)}")
            raise ScrapingError(f"发生意外错误: {str(e)}") from e
            
    def _extract_csv_url(self, page_source: str) -> str:
        """从页面源代码中提取CSV文件的URL
        
        Args:
            page_source: HTML页面源代码
            
        Returns:
            str: CSV文件的完整URL
            
        Raises:
            ScrapingError: 当无法找到CSV链接时抛出
        """
        try:
            # 使用正则表达式查找匹配的URL
            pattern = r'https:\\?/\\?/files\.zillowstatic\.com\\?/research\\?/public_csvs\\?/new_homeowner_affordability\\?/Metro_new_homeowner_affordability.*?\.csv\?t=\d+'
            matches = re.findall(pattern, page_source)
            
            if not matches:
                raise ScrapingError("无法在页面中找到CSV下载链接")
            
            # 获取第一个匹配的URL并清理
            csv_url = matches[0]
            
            # 清理URL（移除多余的反斜杠）
            csv_url = csv_url.replace('\\/', '/')
            
            # 如果URL包含JSON内容，只保留URL部分
            if '"' in csv_url:
                csv_url = csv_url.split('"')[0]
            
            self.logger.info(f"找到CSV URL: {csv_url}")
            return csv_url
            
        except Exception as e:
            self.logger.error(f"提取CSV URL时发生错误: {str(e)}")
            raise ScrapingError(f"提取CSV URL时发生错误: {str(e)}")
            
    def _download_csv(self, url: str, output_path: Path) -> pd.DataFrame:
        """
        Download CSV file from the given URL.
        
        Args:
            url: The URL to download from
            output_path: Path to save the downloaded file
            
        Returns:
            pd.DataFrame: The downloaded data as a DataFrame
        """
        # 确保输出目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 下载文件
        response = requests.get(url, headers=self.session.headers, stream=True)
        response.raise_for_status()
        
        # 获取文件大小
        total_size = int(response.headers.get('content-length', 0))
        
        # 使用tqdm显示下载进度
        with open(output_path, 'wb') as f:
            with tqdm(
                total=total_size,
                unit='iB',
                unit_scale=True,
                unit_divisor=1024,
            ) as pbar:
                for data in response.iter_content(chunk_size=1024):
                    size = f.write(data)
                    pbar.update(size)
                    
        # 读取CSV文件
        try:
            df = pd.read_csv(output_path)
            self.logger.info(f"成功读取CSV文件，包含 {len(df)} 行数据")
            return df
        except Exception as e:
            self.logger.error(f"读取CSV文件失败: {str(e)}")
            if output_path.exists():
                output_path.unlink()
            raise
            
    def _validate_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """
        验证下载的数据。
        
        Args:
            df: 下载的数据
            
        Returns:
            Tuple[bool, Optional[str]]: (是否有效, 错误信息)
        """
        # 检查行数
        if len(df) < 10:  # 根据预期数据大小调整
            return False, f"数据只有 {len(df)} 行，预期至少10行"
            
        # 检查必需的列
        required_cols = [
            'RegionID', 'RegionName', 'RegionType'
        ]
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"缺少必需的列: {missing_cols}"
            
        # 检查空列
        empty_cols = [col for col in df.columns if df[col].isna().all()]
        if empty_cols:
            return False, f"发现完全为空的列: {empty_cols}"
            
        # 检查必需的非空列
        for col in required_cols:
            if df[col].isna().any():
                return False, f"在必需列中发现空值: {col}"
                
        # 检查数据类型
        try:
            # 验证RegionID格式
            df['RegionID'] = pd.to_numeric(df['RegionID'])
            
            # 检查RegionType的值
            valid_region_types = ['country', 'msa']
            invalid_types = df[~df['RegionType'].isin(valid_region_types)]
            if not invalid_types.empty:
                return False, f"发现无效的RegionType值: {invalid_types['RegionType'].unique().tolist()}"
                
            # 检查日期列的格式
            date_cols = [col for col in df.columns if col not in ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']]
            for col in date_cols:
                try:
                    pd.to_datetime(col)
                except ValueError:
                    return False, f"无效的日期列名: {col}"
                    
        except (ValueError, TypeError) as e:
            return False, f"数据类型验证失败: {str(e)}"
            
        return True, None
        
    def scrape(self) -> Path:
        """
        抓取Zillow可负担能力数据。
        
        Returns:
            Path: 保存数据的CSV文件路径
            
        Raises:
            ScrapingError: 当抓取过程失败时抛出
        """
        try:
            # 获取页面源代码
            self.logger.info("开始获取页面源代码")
            page_source = self._get_page_source()
            
            # 提取CSV URL
            self.logger.info("正在提取CSV URL")
            csv_url = self._extract_csv_url(page_source)
            
            # 生成输出文件路径
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = Path("data") / f"zillow_affordability_{timestamp}.csv"
            
            # 下载CSV文件
            self.logger.info(f"开始下载CSV文件到: {output_path}")
            df = self._download_csv(csv_url, output_path)
            
            # 验证数据
            self.logger.info("验证下载的数据")
            is_valid, error_msg = self._validate_data(df)
            if not is_valid:
                raise DataValidationError(f"数据验证失败: {error_msg}")
            
            self.logger.info(f"成功下载并验证数据，保存到: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"抓取过程失败: {str(e)}")
            raise ScrapingError(f"抓取过程失败: {str(e)}") from e 