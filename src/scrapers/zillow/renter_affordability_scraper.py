"""
Zillow Renter Affordability Scraper

This module handles the downloading of new renter affordability data from Zillow.
It includes functionality for downloading CSV data and validating its contents.
"""

import logging
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from tqdm import tqdm

from ...utils.config import Config
from ...utils.exceptions import DataValidationError, ScrapingError

logger = logging.getLogger(__name__)

class RenterAffordabilityScraper:
    """负责从Zillow下载租房者可负担能力数据的类"""
    
    CSV_URL = "https://files.zillowstatic.com/research/public_csvs/new_renter_affordability/Metro_new_renter_affordability_uc_sfrcondomfr_sm_sa_month.csv"
    
    # 浏览器指纹
    BROWSER_SIGNATURES = [
        {
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'accept': 'text/csv,application/csv,text/plain'
        },
        {
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
            'accept': 'text/csv,application/csv,text/plain'
        },
        {
            'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15',
            'accept': 'text/csv,application/csv,text/plain'
        }
    ]

    def __init__(self, config):
        """初始化下载器
        
        Args:
            config: 包含配置信息的对象
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # 确保必要的目录存在
        for path in ["logs", "data"]:
            Path(path).mkdir(exist_ok=True)
        
        self.session = requests.Session()
            
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        before_sleep=before_sleep_log(logger, logging.INFO)
    )
    def _download_csv(self, output_path: Path) -> pd.DataFrame:
        """
        Download CSV file from the Zillow URL.
        
        Args:
            output_path: Path to save the downloaded file
            
        Returns:
            pd.DataFrame: The downloaded data as a DataFrame
            
        Raises:
            ScrapingError: If download fails
        """
        # 确保输出目录存在
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            # 随机选择一个浏览器签名
            browser = random.choice(self.BROWSER_SIGNATURES)
            
            # 设置请求头
            headers = {
                'User-Agent': browser['user_agent'],
                'Accept': browser['accept'],
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache'
            }
            
            # 添加时间戳到URL
            url = f"{self.CSV_URL}?t={int(time.time())}"
            
            # 下载文件
            self.logger.info(f"正在从 {url} 下载文件")
            response = requests.get(
                url,
                headers=headers,
                stream=True,
                timeout=self.config.request_timeout
            )
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
                    desc="下载进度"
                ) as pbar:
                    for data in response.iter_content(chunk_size=8192):
                        size = f.write(data)
                        pbar.update(size)
            
            # 验证文件大小
            if total_size > 0 and output_path.stat().st_size < total_size:
                raise ScrapingError("下载的文件不完整")
                        
            # 读取CSV文件
            try:
                df = pd.read_csv(output_path)
                self.logger.info(f"成功读取CSV文件，包含 {len(df)} 行数据")
                return df
            except pd.errors.EmptyDataError:
                raise DataValidationError("CSV文件为空")
            except pd.errors.ParserError as e:
                raise DataValidationError(f"CSV文件格式错误: {str(e)}")
            
        except requests.RequestException as e:
            self.logger.error(f"下载文件失败: {str(e)}")
            if output_path.exists():
                output_path.unlink()
            raise ScrapingError(f"下载文件失败: {str(e)}") from e
        except Exception as e:
            self.logger.error(f"处理文件失败: {str(e)}")
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
        if len(df) < 200:  # 根据需求文档要求至少200行
            return False, f"数据只有 {len(df)} 行，预期至少200行"
            
        # 检查必需的列
        required_cols = [
            'RegionID', 'RegionName', 'RegionType', 'SizeRank'
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
                
            # 检查日期列的格式和数量
            date_cols = [col for col in df.columns if col not in ['RegionID', 'SizeRank', 'RegionName', 'RegionType', 'StateName']]
            
            # 检查日期列数量（从2015年到现在）
            expected_cols = (datetime.now().year - 2015) * 12 + datetime.now().month
            if len(date_cols) < 100:  # 根据需求文档要求至少100列
                return False, f"日期列数量不足，当前有 {len(date_cols)} 列，预期至少100列"
            
            # 验证日期列格式（支持两种格式：'YYYY-MM-DD' 和 'MM/DD/YYYY'）
            for col in date_cols:
                try:
                    # 尝试 'YYYY-MM-DD' 格式
                    datetime.strptime(col, '%Y-%m-%d')
                except ValueError:
                    try:
                        # 尝试 'MM/DD/YYYY' 格式
                        datetime.strptime(col, '%m/%d/%Y')
                    except ValueError:
                        return False, f"无效的日期列名格式: {col}"
                    
        except (ValueError, TypeError) as e:
            return False, f"数据类型验证失败: {str(e)}"
            
        return True, None
        
    def scrape(self) -> Path:
        """
        下载Zillow租房者可负担能力数据。
        
        Returns:
            Path: 保存数据的CSV文件路径
            
        Raises:
            ScrapingError: 当下载过程失败时抛出
        """
        try:
            # 生成输出文件路径
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = Path("data") / f"zillow_renter_affordability_{timestamp}.csv"
            
            # 下载CSV文件
            self.logger.info(f"开始下载CSV文件到: {output_path}")
            df = self._download_csv(output_path)
            
            # 验证数据
            self.logger.info("验证下载的数据")
            is_valid, error_msg = self._validate_data(df)
            if not is_valid:
                raise DataValidationError(f"数据验证失败: {error_msg}")
            
            self.logger.info(f"成功下载并验证数据，保存到: {output_path}")
            return output_path
            
        except Exception as e:
            self.logger.error(f"下载过程失败: {str(e)}")
            raise ScrapingError(f"下载过程失败: {str(e)}") from e 