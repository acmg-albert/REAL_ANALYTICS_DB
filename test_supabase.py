"""Test Supabase connection and data operations."""

import os
from datetime import datetime
from typing import Dict, List, Optional

import pytest
from dotenv import load_dotenv

from src.utils.config import Config
from src.database.supabase_client import SupabaseClient

# Load environment variables
load_dotenv()

@pytest.fixture(scope="session")
def config() -> Config:
    """Create configuration for testing."""
    return Config.from_env(".env")

@pytest.fixture(scope="session")
def supabase_client(config: Config) -> SupabaseClient:
    """Create Supabase client for testing."""
    return SupabaseClient(
        url=config.supabase_url,
        key=config.supabase_service_role_key
    )

def test_connection(supabase_client: SupabaseClient) -> None:
    """Test database connection."""
    # 使用 service_role_key 创建客户端
    print("Testing database connection...")
    
    # 测试连接
    result = supabase_client.execute_sql("SELECT NOW();")
    assert result is not None
    print("Database connection successful!")

def test_update_logic(supabase_client: SupabaseClient) -> None:
    """Test data update logic."""
    # 准备测试数据
    test_data = [
        {
            "location_name": "Test City 1",
            "location_type": "city",
            "location_fips_code": "12345",
            "rent_estimate": 1500.0,
            "year": 2024,
            "month": 1
        },
        {
            "location_name": "Test City 2",
            "location_type": "city",
            "location_fips_code": "67890",
            "rent_estimate": None,  # 测试空值
            "year": 2024,
            "month": 1
        }
    ]
    
    # 测试插入数据
    print("Testing data insertion...")
    result = supabase_client.execute_sql(
        """
        INSERT INTO apartment_list_rent_estimates 
        (location_name, location_type, location_fips_code, rent_estimate, year, month)
        VALUES 
        (:location_name, :location_type, :location_fips_code, :rent_estimate, :year, :month)
        ON CONFLICT (location_fips_code, year, month) 
        DO UPDATE SET
        rent_estimate = EXCLUDED.rent_estimate
        WHERE apartment_list_rent_estimates.rent_estimate IS NULL 
        OR apartment_list_rent_estimates.rent_estimate < EXCLUDED.rent_estimate
        RETURNING *;
        """,
        params=test_data[0]
    )
    assert result is not None
    print("Data insertion successful!")

if __name__ == "__main__":
    # 设置测试环境
    config = Config.from_env(".env")
    client = SupabaseClient(
        url=config.supabase_url,
        key=config.supabase_service_role_key
    )
    
    # 运行测试
    test_connection(client)
    test_update_logic(client) 