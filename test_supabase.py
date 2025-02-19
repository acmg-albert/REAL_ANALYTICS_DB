import os
from pathlib import Path
from src.database import SupabaseClient
from src.utils.config import Config

def test_connection():
    """Test Supabase connection with both anon and service role keys."""
    print("Loading environment variables...")
    print(f"Current directory: {os.getcwd()}")
    
    # 加载配置
    config = Config.from_env()
    print(f"Loaded config: {config}")
    
    print("Creating Supabase client...")
    db = SupabaseClient(
        url=config.supabase_url,
        key=config.supabase_anon_key
    )
    print("Client created successfully!")
    
    # 检查表是否存在
    print("\nChecking if table exists...")
    if db.check_table_exists():
        # 获取最新的年月
        latest_year_month = db.get_latest_year_month()
        if latest_year_month:
            print(f"Latest year_month in database: {latest_year_month}")
        else:
            print("No data in the table yet.")

if __name__ == "__main__":
    test_connection() 