"""Test Supabase connection and data update logic."""

import os
from pathlib import Path
from src.database import SupabaseClient
from src.utils.config import Config

def test_connection():
    """Test Supabase connection with both anon and service role keys."""
    print("Loading environment variables...")
    print(f"Current directory: {os.getcwd()}")
    
    # 加载配置
    config = Config.from_env(".env")
    print(f"Loaded config: {config}")
    
    print("\nTesting anon key connection...")
    db_anon = SupabaseClient(
        url=config.supabase_url,
        key=config.supabase_anon_key
    )
    print("Anon client created successfully!")
    
    print("\nTesting service role key connection...")
    db_service = SupabaseClient(
        url=config.supabase_url,
        key=config.supabase_service_role_key
    )
    print("Service role client created successfully!")
    
    # 检查表是否存在
    print("\nChecking if table exists...")
    if db_anon.check_table_exists():
        # 获取最新的年月
        latest_year_month = db_anon.get_latest_year_month()
        if latest_year_month:
            print(f"Latest year_month in database: {latest_year_month}")
        else:
            print("No data in the table yet.")

def test_update_logic():
    """Test the new update logic with various scenarios."""
    print("\nTesting update logic...")
    
    # 加载配置
    config = Config.from_env(".env")
    
    # 使用 service_role_key 创建客户端
    print("Using service_role_key for testing...")
    db = SupabaseClient(
        url=config.supabase_url,
        key=config.supabase_service_role_key
    )
    
    # 测试数据
    test_records = [
        # 新的 location
        {
            'location_name': 'Test City 1',
            'location_type': 'city',
            'location_fips_code': 'TEST001',
            'population': 100000,
            'state': 'TS',
            'county': 'Test County',
            'metro': 'Test Metro',
            'year_month': '2024_02',
            'rent_estimate_overall': 1500,
            'rent_estimate_1br': 1200,
            'rent_estimate_2br': 1800
        },
        # 现有 location 的新月份
        {
            'location_name': 'New York',
            'location_type': 'city',
            'location_fips_code': '36061',
            'population': 8336817,
            'state': 'NY',
            'county': 'New York',
            'metro': 'New York-Newark-Jersey City',
            'year_month': '2024_03',
            'rent_estimate_overall': 3600,
            'rent_estimate_1br': 2900,
            'rent_estimate_2br': 3300
        },
        # 现有记录的更新（非空值）
        {
            'location_name': 'New York',
            'location_type': 'city',
            'location_fips_code': '36061',
            'population': 8336817,
            'state': 'NY',
            'county': 'New York',
            'metro': 'New York-Newark-Jersey City',
            'year_month': '2024_02',
            'rent_estimate_overall': 3550,
            'rent_estimate_1br': 2850,
            'rent_estimate_2br': 3250
        },
        # 现有记录的更新（空值，不应更新）
        {
            'location_name': 'Los Angeles',
            'location_type': 'city',
            'location_fips_code': '06037',
            'population': 3898747,
            'state': 'CA',
            'county': 'Los Angeles',
            'metro': 'Los Angeles-Long Beach-Anaheim',
            'year_month': '2024_02',
            'rent_estimate_overall': None,
            'rent_estimate_1br': 0,
            'rent_estimate_2br': None
        }
    ]
    
    # 执行更新
    print("\nInserting test records...")
    count = db.insert_rent_estimates(test_records)
    print(f"Processed {count} records")

if __name__ == "__main__":
    test_connection()
    test_update_logic() 