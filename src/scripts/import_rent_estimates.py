"""Import rent estimates from processed data into Supabase."""

import logging
import os
from pathlib import Path
import pandas as pd
from tqdm import tqdm

from src.database import SupabaseClient
from src.utils.config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def find_latest_processed_file(data_dir: Path) -> Path:
    """Find the latest processed rent estimates file."""
    # 首先检查processed目录
    processed_dir = data_dir / 'processed'
    if processed_dir.exists():
        csv_files = list(processed_dir.glob("rent_estimates_*.csv"))
        if csv_files:
            return max(csv_files, key=lambda p: p.stat().st_mtime)
            
    # 如果processed目录没有文件，检查data目录
    csv_files = list(data_dir.glob("rent_estimates_processed_*.csv"))
    if csv_files:
        return max(csv_files, key=lambda p: p.stat().st_mtime)
        
    # 如果还是没有找到，返回默认路径
    return data_dir / 'processed' / 'rent_estimates.csv'

def import_data_in_batches(db: SupabaseClient, df: pd.DataFrame, batch_size: int = 1000) -> int:
    """Import data in batches to avoid memory issues."""
    total_rows = len(df)
    imported_count = 0
    
    # 使用tqdm创建进度条
    with tqdm(total=total_rows, desc="Importing data") as pbar:
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            # 转换为字典列表
            records = batch_df.to_dict('records')
            
            # 使用upsert导入数据
            result = db.insert_rent_estimates(records)
            
            # 更新进度
            imported_count += result
            pbar.update(len(batch_df))
            
    return imported_count

def main() -> int:
    """Main function to import rent estimates data."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Initialize database client with service role key for write access
        db = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key  # 使用service_role_key进行写操作
        )
        
        # Get the data directory
        data_dir = Path(__file__).parent.parent.parent / 'data'
        if not data_dir.exists():
            logger.error(f"Data directory not found: {data_dir}")
            logger.info("Creating data directory structure...")
            data_dir.mkdir(parents=True, exist_ok=True)
            (data_dir / 'raw').mkdir(exist_ok=True)
            (data_dir / 'processed').mkdir(exist_ok=True)
            return 1
            
        # Find the latest processed file
        processed_file = find_latest_processed_file(data_dir)
        logger.info(f"Looking for processed file at: {processed_file}")
        
        if not processed_file.exists():
            logger.error(f"Processed data file not found: {processed_file}")
            logger.info("Available files in data directory:")
            for file in data_dir.rglob("*.csv"):
                logger.info(f"- {file.relative_to(data_dir)}")
            return 1
            
        # Read the CSV file
        logger.info(f"Reading data from {processed_file}")
        df = pd.read_csv(processed_file)
        
        # 处理可能的空值
        numeric_columns = ['population', 'rent_estimate_overall', 'rent_estimate_1br', 'rent_estimate_2br']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = df[col].fillna(0)
                
        string_columns = ['location_name', 'location_type', 'location_fips_code', 
                         'state', 'county', 'metro', 'year_month']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('')
                
        # Import data in batches
        logger.info(f"Starting data import from {processed_file}")
        total_imported = import_data_in_batches(db, df)
        
        logger.info(f"Successfully imported {total_imported} records")
        return 0
        
    except Exception as e:
        logger.error(f"Failed to import data: {e}")
        return 1

if __name__ == "__main__":
    exit(main()) 