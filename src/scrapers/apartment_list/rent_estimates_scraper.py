"""
Apartment List Rent Estimates Scraper

This module handles the scraping of rent estimates data from Apartment List.
It includes functionality for downloading CSV data and validating its contents.
"""

import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

from ...utils.config import Config
from ...utils.exceptions import DataValidationError, ScrapingError

logger = logging.getLogger(__name__)

class RentEstimatesScraper:
    """Scraper for Apartment List rent estimates data."""
    
    BASE_URL = "https://www.apartmentlist.com/research/category/data-rent-estimates"
    
    def __init__(self, config: Config):
        """Initialize the scraper with configuration."""
        self.config = config
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0"
        })
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
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
            # Add a random delay
            time.sleep(self.config.scraping_delay)
            
            response = self.session.get(self.BASE_URL, timeout=self.config.request_timeout)
            response.raise_for_status()
            
            # Save response for debugging
            debug_file = Path("logs") / "response.html"
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            logger.debug(f"Response saved to {debug_file}")
            
            return response.text
            
        except requests.RequestException as e:
            raise ScrapingError(f"Failed to fetch page: {str(e)}") from e
            
    def _extract_csv_url(self, page_source: str) -> str:
        """
        Extract the CSV URL from the page source.
        
        Args:
            page_source (str): The HTML page source
        
        Returns:
            str: The URL of the rent estimates CSV file
        
        Raises:
            ScrapingError: If unable to find the CSV URL
        """
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Debug: Print all links
        logger.debug("Found links:")
        for link in soup.find_all(['a', 'link', 'script', 'img'], href=True):
            href = link.get('href', link.get('src', ''))
            logger.debug(f"Link: {href}")
            
            # Try different patterns
            patterns = [
                r'.*Apartment_List_Rent_Estimates.*\.csv',
                r'.*rent.*estimates.*\.csv',
                r'.*apartment.*list.*rent.*\.csv',
                r'//assets\.ctfassets\.net/.*\.csv'
            ]
            
            for pattern in patterns:
                if re.search(pattern, href, re.IGNORECASE) and 'Summary' not in href:
                    url = urljoin(self.BASE_URL, href)
                    logger.debug(f"Found potential URL: {url}")
                    return url
                    
        # Try finding in text
        text = soup.get_text()
        patterns = [
            r'https?://[^\s<>"]+?/[^\s<>"]+?Apartment_List_Rent_Estimates[^\s<>"]+?\.csv',
            r'https?://[^\s<>"]+?/[^\s<>"]+?rent[^\s<>"]+?estimates[^\s<>"]+?\.csv',
            r'//[^\s<>"]+?/[^\s<>"]+?Apartment_List_Rent_Estimates[^\s<>"]+?\.csv'
        ]
        
        for pattern in patterns:
            urls = re.findall(pattern, text, re.IGNORECASE)
            for url in urls:
                if 'Summary' not in url:
                    logger.debug(f"Found potential URL in text: {url}")
                    return f"https:{url}" if url.startswith('//') else url
                    
        # Try finding in scripts
        for script in soup.find_all('script'):
            script_text = script.string
            if script_text:
                for pattern in patterns:
                    urls = re.findall(pattern, script_text, re.IGNORECASE)
                    for url in urls:
                        if 'Summary' not in url:
                            logger.debug(f"Found potential URL in script: {url}")
                            return f"https:{url}" if url.startswith('//') else url
                            
        raise ScrapingError("Could not find rent estimates CSV URL in page source")
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _download_csv(self, url: str, output_path: Path) -> None:
        """
        Download the CSV file with progress bar.
        
        Args:
            url (str): The URL of the CSV file
            output_path (Path): Where to save the CSV file
        
        Raises:
            ScrapingError: If download fails
        """
        try:
            # Add a random delay
            time.sleep(self.config.scraping_delay)
            
            response = self.session.get(url, stream=True, timeout=self.config.request_timeout)
            response.raise_for_status()
            
            # Get file size for progress bar
            total_size = int(response.headers.get('content-length', 0))
            
            # Ensure directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Download with progress bar
            with open(output_path, 'wb') as f, tqdm(
                desc="Downloading rent estimates data",
                total=total_size,
                unit='iB',
                unit_scale=True
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    size = f.write(chunk)
                    pbar.update(size)
                    
        except requests.RequestException as e:
            if output_path.exists():
                output_path.unlink()
            raise ScrapingError(f"Failed to download CSV: {str(e)}") from e
            
    def _validate_data(self, df: pd.DataFrame) -> Tuple[bool, Optional[str]]:
        """
        Validate the downloaded data.
        
        Args:
            df (pd.DataFrame): The downloaded data
            
        Returns:
            Tuple[bool, Optional[str]]: (is_valid, error_message)
        """
        # Check row count
        if len(df) < 3000:
            return False, f"Data has only {len(df)} rows, expected at least 3000"
            
        # Check column count and types
        required_cols = [
            'location_name', 'location_type', 'location_fips_code',
            'population', 'state', 'county', 'metro', 'bed_size'
        ]
        
        for col in required_cols:
            if col not in df.columns:
                return False, f"Missing required column: {col}"
                
        # Check for empty columns
        empty_cols = [col for col in df.columns if df[col].isna().all()]
        if empty_cols:
            return False, f"Found completely empty columns: {empty_cols}"
            
        # Check for required non-null columns
        for col in ['location_name', 'location_type', 'location_fips_code', 'bed_size']:
            if df[col].isna().any():
                return False, f"Found null values in required column: {col}"
                
        # Check data types
        try:
            # Convert population to numeric
            df['population'] = pd.to_numeric(df['population'])
            
            # Check rent columns (YYYY_MM format)
            rent_cols = [col for col in df.columns if re.match(r'^\d{4}_\d{2}$', col)]
            if not rent_cols:
                return False, "No rent estimate columns found"
                
            # Verify rent values are numeric and non-negative
            for col in rent_cols:
                df[col] = pd.to_numeric(df[col])
                if (df[col] < 0).any():
                    return False, f"Found negative rent values in column: {col}"
                    
        except (ValueError, TypeError) as e:
            return False, f"Data type validation failed: {str(e)}"
            
        return True, None
        
    def scrape(self) -> Path:
        """
        Scrape the rent estimates data.
        
        Returns:
            Path: Path to the downloaded CSV file
        
        Raises:
            ScrapingError: If scraping fails
            DataValidationError: If data validation fails
        """
        logger.info("Starting rent estimates scraping")
        
        # Create timestamp for file naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(self.config.data_dir) / f"rent_estimates_{timestamp}.csv"
        
        # Get page source and extract CSV URL
        page_source = self._get_page_source()
        
        # Debug: Print page source length and save it
        logger.info(f"Retrieved page source (length: {len(page_source)})")
        debug_file = Path("logs") / "page_source.html"
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(page_source)
        logger.debug(f"Page source saved to {debug_file}")
        
        csv_url = self._extract_csv_url(page_source)
        
        logger.info(f"Found rent estimates CSV URL: {csv_url}")
        
        # Download the CSV file
        self._download_csv(csv_url, output_path)
        
        # Read and validate the data
        try:
            df = pd.read_csv(output_path)
            is_valid, error_msg = self._validate_data(df)
            
            if not is_valid:
                raise DataValidationError(f"Data validation failed: {error_msg}")
                
            logger.info("Data validation successful")
            return output_path
            
        except Exception as e:
            if output_path.exists():
                output_path.unlink()
            raise DataValidationError(f"Failed to validate data: {str(e)}") from e 