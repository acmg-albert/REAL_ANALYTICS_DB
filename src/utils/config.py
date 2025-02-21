"""Configuration management for the application."""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from dotenv import load_dotenv

from .exceptions import ConfigurationError

@dataclass
class Config:
    """Application configuration."""
    
    # Database configuration
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    
    # Supabase configuration
    supabase_url: str
    supabase_anon_key: str
    
    # API configuration
    apartment_list_api_base_url: str
    
    # Scraping configuration
    scraping_delay: float
    max_retries: int
    request_timeout: int
    user_agent: str
    
    # Paths
    data_dir: Path
    log_dir: Path
    
    # Logging
    log_level: str
    
    # Time On Market Data Validation
    min_time_on_market_rows: int = 100
    min_time_on_market_base_columns: int = 7  # 基本列：location_name, location_type, location_fips_code, population, state, county, metro
    min_time_on_market_months: int = 73  # 最少应该有的月份数（从2019年1月到2025年1月）
    earliest_time_on_market_year: int = 2019
    earliest_time_on_market_month: int = 1
    
    # Optional configurations
    apartment_list_api_key: Optional[str] = None
    supabase_service_role_key: Optional[str] = None
    
    @staticmethod
    def _validate_supabase_url(url: str) -> str:
        """
        Validate and normalize Supabase URL.
        
        Args:
            url: Raw Supabase URL
            
        Returns:
            str: Normalized URL
            
        Raises:
            ConfigurationError: If URL is invalid
        """
        try:
            print(f"Validating Supabase URL: {url}")
            # Basic validation
            if not url.startswith('https://') or not url.endswith('.supabase.co'):
                raise ValueError("URL must be in format: https://<project>.supabase.co")
                
            # Remove trailing slash if present
            return url.rstrip('/')
            
        except Exception as e:
            raise ConfigurationError(f"Invalid Supabase URL: {str(e)}") from e
    
    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> 'Config':
        """
        Create configuration from environment variables.
        
        Args:
            env_file: Optional path to .env file
            
        Returns:
            Config: Configuration instance
            
        Raises:
            ConfigurationError: If required configuration is missing
        """
        # 强制重新加载环境变量
        load_dotenv(override=True)
        
        if env_file:
            if not os.path.exists(env_file):
                raise ConfigurationError(f"Environment file not found: {env_file}")
            print(f"Loading environment variables from: {env_file}")
            load_dotenv(env_file, override=True)
            
        try:
            # Create data and log directories
            data_dir = Path("data")
            log_dir = Path("logs")
            data_dir.mkdir(exist_ok=True)
            log_dir.mkdir(exist_ok=True)
            
            # Get Supabase configuration
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_anon_key = os.getenv("SUPABASE_ANON_KEY") or os.getenv("SUPABASE_KEY")
            supabase_service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            
            print(f"Raw SUPABASE_URL: {supabase_url}")
            print(f"Raw SUPABASE_ANON_KEY: {supabase_anon_key}")
            print(f"Raw SUPABASE_SERVICE_ROLE_KEY: {supabase_service_role_key}")
            
            if not supabase_url or not supabase_anon_key:
                raise ConfigurationError("SUPABASE_URL and SUPABASE_ANON_KEY are required")
                
            # Validate and normalize Supabase URL
            supabase_url = cls._validate_supabase_url(supabase_url)
            
            return cls(
                # Database configuration
                db_host=os.getenv("DB_HOST", "localhost"),
                db_port=int(os.getenv("DB_PORT", "5432")),
                db_name=os.getenv("DB_NAME", "real_estate_analytics"),
                db_user=os.getenv("DB_USER", ""),
                db_password=os.getenv("DB_PASSWORD", ""),
                
                # Supabase configuration
                supabase_url=supabase_url,
                supabase_anon_key=supabase_anon_key,
                
                # API configuration
                apartment_list_api_base_url=os.getenv(
                    "APARTMENT_LIST_API_BASE_URL",
                    "https://www.apartmentlist.com/api/v2"
                ),
                
                # Scraping configuration
                scraping_delay=float(os.getenv("SCRAPING_DELAY", "2.0")),
                max_retries=int(os.getenv("MAX_RETRIES", "3")),
                request_timeout=int(os.getenv("TIMEOUT", "30")),
                user_agent=os.getenv(
                    "USER_AGENT",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ),
                
                # Paths
                data_dir=data_dir,
                log_dir=log_dir,
                
                # Logging
                log_level=os.getenv("LOG_LEVEL", "INFO"),
                
                # Optional configurations
                apartment_list_api_key=os.getenv("APARTMENT_LIST_API_KEY"),
                supabase_service_role_key=supabase_service_role_key,
            )
            
        except (ValueError, TypeError) as e:
            raise ConfigurationError(f"Invalid configuration: {str(e)}") from e 