"""Script to schedule data updates."""

import logging
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

# 创建调度器
scheduler = BackgroundScheduler()

def update_database_views(config: Config):
    """Update database views."""
    try:
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 直接刷新所有视图
        views = [
            'apartment_list_rent_estimates_view',
            'apartment_list_vacancy_index_view',
            'apartment_list_time_on_market_view'
        ]
        
        results = []
        for view_name in views:
            try:
                refresh_sql = f"REFRESH MATERIALIZED VIEW {view_name};"
                result = supabase.client.rpc('raw_sql', {'command': refresh_sql}).execute()
                
                if result.data and result.data.get('status') == 'success':
                    logger.info(f"已刷新视图: {view_name}")
                    results.append({'view': view_name, 'status': 'success'})
                else:
                    logger.error(f"刷新视图失败: {view_name}")
                    logger.error(f"错误信息: {result.data}")
                    results.append({'view': view_name, 'status': 'error', 'error': str(result.data)})
            except Exception as e:
                error_msg = f"刷新视图 {view_name} 时发生错误: {str(e)}"
                logger.error(error_msg)
                results.append({'view': view_name, 'status': 'error', 'error': error_msg})
                
        return results
                
    except Exception as e:
        error_msg = f"更新视图时发生错误: {e}"
        logger.error(error_msg)
        return [{'status': 'error', 'error': error_msg}]

def run_daily_update():
    """Run daily update tasks."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # 刷新物化视图
        logger.info("开始刷新物化视图")
        results = update_database_views(config)
        
        logger.info("每日更新完成")
        return results
        
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
    results = run_daily_update()
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