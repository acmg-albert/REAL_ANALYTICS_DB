import os
from pathlib import Path
from src.database import SupabaseClient
from src.utils.config import Config

def test_connection():
    # Set environment variables directly
    os.environ["SUPABASE_URL"] = "https://qjdvcnyxsnsfsdhxfxlm.supabase.co"
    os.environ["SUPABASE_KEY"] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFqZHZjbnl4c25zZnNkaHhmeGxtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mzk3MjQxOTIsImV4cCI6MjA1NTMwMDE5Mn0.KDve7Zj1s_HKjR_jKPE0F6djdRZQRg8E5UqBeyi35Io"
    
    print("Loading environment variables...")
    print(f"Current directory: {os.getcwd()}")
    print(f"SUPABASE_URL: {os.getenv('SUPABASE_URL')}")
    print(f"SUPABASE_KEY: {os.getenv('SUPABASE_KEY')}")
    
    config = Config.from_env()
    print(f"Loaded config: {config}")
    
    print("Creating Supabase client...")
    db = SupabaseClient(config.supabase_url, config.supabase_key)
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