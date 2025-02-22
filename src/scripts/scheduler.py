"""Script to schedule data updates."""

import logging
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from ..utils.config import Config
from ..database import SupabaseClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

def update_database_views(config: Config):
    """Update database views."""
    try:
        # Initialize Supabase client
        supabase = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # 检查并刷新rent_estimates视图
        check_sql = """
        SELECT last_refresh
        FROM pg_stat_user_tables
        WHERE matviewname = 'apartment_list_rent_estimates_view'
        AND last_refresh < NOW() - INTERVAL '1 hour';
        """
        result = supabase.client.rpc('raw_sql', {'command': check_sql}).execute()
        if result.data:
            refresh_sql = "REFRESH MATERIALIZED VIEW apartment_list_rent_estimates_view;"
            supabase.client.rpc('raw_sql', {'command': refresh_sql}).execute()
            logger.info("已刷新rent_estimates视图")
            
        # 检查并刷新vacancy_index视图
        check_sql = """
        SELECT last_refresh
        FROM pg_stat_user_tables
        WHERE matviewname = 'apartment_list_vacancy_index_view'
        AND last_refresh < NOW() - INTERVAL '1 hour';
        """
        result = supabase.client.rpc('raw_sql', {'command': check_sql}).execute()
        if result.data:
            refresh_sql = "REFRESH MATERIALIZED VIEW apartment_list_vacancy_index_view;"
            supabase.client.rpc('raw_sql', {'command': refresh_sql}).execute()
            logger.info("已刷新vacancy_index视图")
            
        # 检查并刷新time_on_market视图
        check_sql = """
        SELECT last_refresh
        FROM pg_stat_user_tables
        WHERE matviewname = 'apartment_list_time_on_market_view'
        AND last_refresh < NOW() - INTERVAL '1 hour';
        """
        result = supabase.client.rpc('raw_sql', {'command': check_sql}).execute()
        if result.data:
            refresh_sql = "REFRESH MATERIALIZED VIEW apartment_list_time_on_market_view;"
            supabase.client.rpc('raw_sql', {'command': refresh_sql}).execute()
            logger.info("已刷新time_on_market视图")
            
    except Exception as e:
        logger.error(f"更新视图时发生错误: {e}")

def run_daily_update():
    """Run daily update tasks."""
    try:
        # Load configuration
        config = Config.from_env()
        
        # Update views
        update_database_views(config)
        
        logger.info("每日更新完成")
        
    except Exception as e:
        logger.error(f"每日更新失败: {e}")

# 创建调度器
scheduler = BackgroundScheduler()
scheduler.add_job(run_daily_update, 'interval', hours=1)

@app.route('/')
def home():
    """Home page."""
    return {
        'status': 'running',
        'time': datetime.now().isoformat()
    }

@app.route('/run-update')
def trigger_update():
    """Manually trigger update."""
    run_daily_update()
    return {'status': 'update triggered'}

def main():
    """Main entry point."""
    try:
        # Start scheduler
        scheduler.start()
        
        # Run Flask app
        app.run(host='0.0.0.0', port=int(sys.argv[1]) if len(sys.argv) > 1 else 8000)
        
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()

if __name__ == "__main__":
    main() 