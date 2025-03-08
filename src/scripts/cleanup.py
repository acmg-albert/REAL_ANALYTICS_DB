"""Script to clean up old data files."""

import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
import shutil
from typing import List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def get_old_files(directory: Path, pattern: str, days: int) -> List[Path]:
    """
    Get list of files older than specified days.
    
    Args:
        directory: Directory to search
        pattern: File pattern to match
        days: Number of days old
        
    Returns:
        List[Path]: List of old files
    """
    cutoff = datetime.now() - timedelta(days=days)
    return [
        f for f in directory.glob(pattern)
        if f.stat().st_mtime < cutoff.timestamp()
    ]

def cleanup_data_files(data_dir: Path, retention_days: dict) -> None:
    """
    Clean up old data files.
    
    Args:
        data_dir: Data directory
        retention_days: Dictionary of file patterns and retention days
    """
    # 确保目录存在
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # 清理每种类型的文件
    for pattern, days in retention_days.items():
        old_files = get_old_files(data_dir, pattern, days)
        for file in old_files:
            try:
                file.unlink()
                logger.info(f"Deleted old file: {file}")
            except Exception as e:
                logger.error(f"Failed to delete {file}: {e}")

def cleanup_logs(log_dir: Path, retention_days: int) -> None:
    """
    Clean up old log files.
    
    Args:
        log_dir: Log directory
        retention_days: Number of days to retain logs
    """
    # 确保目录存在
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # 清理旧日志文件
    old_logs = get_old_files(log_dir, "*.log", retention_days)
    for log in old_logs:
        try:
            log.unlink()
            logger.info(f"Deleted old log: {log}")
        except Exception as e:
            logger.error(f"Failed to delete {log}: {e}")

def main():
    """Main entry point for cleanup script."""
    try:
        # 定义数据保留策略
        data_retention = {
            # 原始数据文件
            "rent_estimates_2*.csv": 7,  # 7天
            "vacancy_index_2*.csv": 7,
            "time_on_market_2*.csv": 7,
            "zillow_affordability_2*.csv": 7,
            "zillow_renter_affordability_2*.csv": 7,
            
            # 处理后的数据文件
            "*processed*.csv": 30,  # 30天
        }
        
        # 清理数据文件
        data_dir = Path("data")
        cleanup_data_files(data_dir, data_retention)
        
        # 清理日志文件（保留30天）
        log_dir = Path("logs")
        cleanup_logs(log_dir, 30)
        
        logger.info("Cleanup completed successfully")
        return 0
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 