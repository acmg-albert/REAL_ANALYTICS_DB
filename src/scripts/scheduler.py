"""Script to schedule data updates."""

import logging
import sys
import subprocess
import os
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify

from ..utils.config import Config
from ..database import (
    RentEstimatesClient,
    VacancyIndexClient,
    TimeOnMarketClient,
    HomeownerAffordabilityClient,
    RenterAffordabilityClient
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

# 创建调度器
scheduler = BackgroundScheduler()

def run_script(script_name: str) -> bool:
    """运行指定的Python脚本。"""
    try:
        logger.info(f"开始执行脚本: {script_name}")
        result = subprocess.run(
            [sys.executable, "-m", f"src.scripts.{script_name}"],
            check=True,
            capture_output=True,
            text=True,
            env=os.environ.copy()  # 确保环境变量被正确传递
        )
        logger.info(f"脚本 {script_name} 执行完成")
        if result.stdout:
            logger.info(f"脚本输出: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"脚本 {script_name} 执行失败")
        if e.stdout:
            logger.error(f"标准输出: {e.stdout}")
        if e.stderr:
            logger.error(f"错误输出: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"执行脚本 {script_name} 时发生未知错误: {str(e)}")
        return False

def update_database_views(config: Config):
    """Update database views."""
    try:
        # Initialize clients
        clients = [
            RentEstimatesClient(url=config.supabase_url, key=config.supabase_service_role_key),
            VacancyIndexClient(url=config.supabase_url, key=config.supabase_service_role_key),
            TimeOnMarketClient(url=config.supabase_url, key=config.supabase_service_role_key),
            HomeownerAffordabilityClient(url=config.supabase_url, key=config.supabase_service_role_key),
            RenterAffordabilityClient(url=config.supabase_url, key=config.supabase_service_role_key)
        ]
        
        results = []
        for client in clients:
            try:
                client.refresh_materialized_view(client.VIEW_NAME)
                logger.info(f"已刷新视图: {client.VIEW_NAME}")
                results.append({'status': 'success', 'view': client.VIEW_NAME})
            except Exception as e:
                error_msg = f"刷新视图 {client.VIEW_NAME} 时发生错误: {str(e)}"
                logger.error(error_msg)
                results.append({'status': 'error', 'view': client.VIEW_NAME, 'error': error_msg})
                
        return results
                
    except Exception as e:
        error_msg = f"更新视图时发生错误: {e}"
        logger.error(error_msg)
        return [{'status': 'error', 'error': error_msg}]

def run_full_update() -> list:
    """运行完整的数据更新流程。"""
    results = []
    
    # 运行所有数据处理脚本
    scripts = [
        'scrape_apartment_list_rent_estimates',
        'process_apartment_list_rent_estimates',
        'import_apartment_list_rent_estimates',
        'scrape_apartment_list_vacancy_index',
        'process_apartment_list_vacancy_index',
        'import_apartment_list_vacancy_index',
        'scrape_apartment_list_time_on_market',
        'process_apartment_list_time_on_market',
        'import_apartment_list_time_on_market',
        'scrape_zillow_affordability',
        'process_zillow_affordability',
        'import_zillow_affordability',
        'scrape_zillow_renter_affordability',
        'process_zillow_renter_affordability',
        'import_zillow_renter_affordability'
    ]
    
    for script in scripts:
        success = run_script(script)
        results.append({
            'script': script,
            'status': 'success' if success else 'error'
        })
    
    # 加载配置
    config = Config.from_env()
    
    # 刷新物化视图
    logger.info("开始刷新物化视图")
    view_results = update_database_views(config)
    results.extend(view_results)
    
    logger.info("完整数据更新流程完成")
    return results

def run_daily_update():
    """Run daily update tasks."""
    try:
        return run_full_update()
    except Exception as e:
        error_msg = f"每日更新失败: {e}"
        logger.error(error_msg)
        return {'status': 'error', 'error': error_msg}

def init_scheduler():
    """Initialize the scheduler."""
    if not scheduler.running:
        scheduler.add_job(run_daily_update, 'interval', hours=1)
        scheduler.start()
        logger.info("调度器已启动")

@app.route('/')
def home():
    """Home page."""
    # 确保调度器已启动
    init_scheduler()
    
    return jsonify({
        'status': 'running',
        'time': datetime.now().isoformat(),
        'scheduler_status': 'running' if scheduler.running else 'not running'
    })

@app.route('/run-update')
def trigger_update():
    """Manually trigger update."""
    logger.info("手动触发完整数据更新流程")
    results = run_full_update()
    return jsonify({
        'status': 'update triggered',
        'results': results,
        'time': datetime.now().isoformat()
    })

def main():
    """Main entry point."""
    try:
        # Run Flask app
        port = int(sys.argv[1]) if len(sys.argv) > 1 else 8000
        app.run(host='0.0.0.0', port=port)
        
    except (KeyboardInterrupt, SystemExit):
        if scheduler.running:
            scheduler.shutdown()

if __name__ == "__main__":
    main() 