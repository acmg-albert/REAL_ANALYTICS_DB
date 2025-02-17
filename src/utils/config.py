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
    supabase_key: str
    
    # API configuration
    apartment_list_api_key: Optional[str]
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
        if env_file:
            if not os.path.exists(env_file):
                raise ConfigurationError(f"Environment file not found: {env_file}")
            print(f"Loading environment variables from: {env_file}")
            load_dotenv(env_file)
            
        try:
            # Create data and log directories
            data_dir = Path("data")
            log_dir = Path("logs")
            data_dir.mkdir(exist_ok=True)
            log_dir.mkdir(exist_ok=True)
            
            # Get Supabase configuration
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_KEY")
            
            print(f"Raw SUPABASE_URL: {supabase_url}")
            print(f"Raw SUPABASE_KEY: {supabase_key}")
            
            if not supabase_url or not supabase_key:
                raise ConfigurationError("SUPABASE_URL and SUPABASE_KEY are required")
                
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
                supabase_key=supabase_key,
                
                # API configuration
                apartment_list_api_key=os.getenv("APARTMENT_LIST_API_KEY"),
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
            )
            
        except (ValueError, TypeError) as e:
            raise ConfigurationError(f"Invalid configuration: {str(e)}") from e 