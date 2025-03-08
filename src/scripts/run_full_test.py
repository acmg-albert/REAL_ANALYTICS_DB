"""Script to run full data pipeline test locally."""

import logging
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'logs/full_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

logger = logging.getLogger(__name__)

def run_script(script_name: str) -> Tuple[bool, str]:
    """
    Run a Python script and return its status.
    
    Args:
        script_name: Name of the script to run
        
    Returns:
        Tuple[bool, str]: (success, message)
    """
    try:
        logger.info(f"Running {script_name}...")
        result = subprocess.run(
            [sys.executable, '-m', f'src.scripts.{script_name}'],
            capture_output=True,
            text=True,
            check=True
        )
        return True, f"{script_name} completed successfully"
    except subprocess.CalledProcessError as e:
        error_msg = f"Error in {script_name}: {e.stderr}"
        logger.error(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error in {script_name}: {str(e)}"
        logger.error(error_msg)
        return False, error_msg

def main():
    """Main entry point for running full test."""
    # 确保必要的目录存在
    Path('data/raw').mkdir(parents=True, exist_ok=True)
    Path('data/processed').mkdir(parents=True, exist_ok=True)
    Path('logs').mkdir(exist_ok=True)
    
    # 定义要执行的脚本列表
    scripts = [
        # ApartmentList Rent Estimates
        'scrape_apartment_list_rent_estimates',
        'process_apartment_list_rent_estimates',
        'import_apartment_list_rent_estimates',
        
        # ApartmentList Vacancy Index
        'scrape_apartment_list_vacancy_index',
        'process_apartment_list_vacancy_index',
        'import_apartment_list_vacancy_index',
        
        # ApartmentList Time on Market
        'scrape_apartment_list_time_on_market',
        'process_apartment_list_time_on_market',
        'import_apartment_list_time_on_market',
        
        # Zillow Homeowner Affordability
        'scrape_zillow_affordability',
        'process_zillow_affordability',
        'import_zillow_affordability',
        
        # Zillow Renter Affordability
        'scrape_zillow_renter_affordability',
        'process_zillow_renter_affordability',
        'import_zillow_renter_affordability'
    ]
    
    # 执行所有脚本
    results = []
    total_scripts = len(scripts)
    failed_scripts = []
    
    logger.info(f"Starting full test with {total_scripts} scripts")
    print(f"\n{'='*80}\nStarting full test with {total_scripts} scripts\n{'='*80}\n")
    
    for i, script in enumerate(scripts, 1):
        print(f"\n[{i}/{total_scripts}] Running {script}...")
        success, message = run_script(script)
        results.append((script, success, message))
        
        if not success:
            failed_scripts.append(script)
            print(f"❌ {script} failed: {message}")
        else:
            print(f"✅ {script} completed successfully")
    
    # 打印总结报告
    print(f"\n{'='*80}\nTest Summary\n{'='*80}")
    print(f"\nTotal scripts: {total_scripts}")
    print(f"Successful: {total_scripts - len(failed_scripts)}")
    print(f"Failed: {len(failed_scripts)}")
    
    if failed_scripts:
        print("\nFailed scripts:")
        for script in failed_scripts:
            print(f"❌ {script}")
        return 1
    else:
        print("\n✅ All scripts completed successfully!")
        return 0

if __name__ == '__main__':
    sys.exit(main()) 