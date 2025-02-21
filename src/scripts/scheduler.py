"""Scheduler for automated data updates."""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from flask import Flask

from src.database import SupabaseClient
from src.utils.config import Config
from src.scripts.scrape_rent_estimates import main as scrape_rent_main
from src.scripts.process_rent_estimates import main as process_rent_main
from src.scripts.import_rent_estimates import main as import_rent_main
from src.scripts.scrape_vacancy_index import main as scrape_vacancy_main
from src.scripts.process_vacancy_index import main as process_vacancy_main
from src.scripts.import_vacancy_index import main as import_vacancy_main

# 创建Flask应用（Render需要一个web服务）
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def update_database_views(config: Config):
    """Update all database views."""
    try:
        # 使用service_role_key进行数据库操作
        db = SupabaseClient(
            url=config.supabase_url,
            key=config.supabase_service_role_key
        )
        
        # SQL to check if views exist and refresh them
        sql = """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 
                FROM pg_matviews 
                WHERE matviewname = 'db_view_apartment_list_rent_estimates_1_3'
            ) THEN
                REFRESH MATERIALIZED VIEW db_view_apartment_list_rent_estimates_1_3;
            END IF;
            
            IF EXISTS (
                SELECT 1 
                FROM pg_matviews 
                WHERE matviewname = 'db_view_apartment_list_vacancy_index_1_3'
            ) THEN
                REFRESH MATERIALIZED VIEW db_view_apartment_list_vacancy_index_1_3;
            END IF;
        END $$;
        """
        
        # Execute the SQL
        db.execute_sql(sql)
        logger.info("Successfully updated database views")
        
    except Exception as e:
        logger.error(f"Failed to update database views: {e}")
        raise

def run_daily_update():
    """Run the daily update process."""
    try:
        logger.info("Starting daily update process")
        
        # Load configuration
        config = Config.from_env()
        
        # Run the rent estimates update pipeline
        logger.info("Updating rent estimates data...")
        scrape_result = scrape_rent_main()
        if scrape_result != 0:
            raise Exception("Rent estimates scraping failed")
            
        process_result = process_rent_main()
        if process_result != 0:
            raise Exception("Rent estimates processing failed")
            
        import_result = import_rent_main()
        if import_result != 0:
            raise Exception("Rent estimates import failed")
        
        # Run the vacancy index update pipeline
        logger.info("Updating vacancy index data...")
        scrape_result = scrape_vacancy_main()
        if scrape_result != 0:
            raise Exception("Vacancy index scraping failed")
            
        process_result = process_vacancy_main()
        if process_result != 0:
            raise Exception("Vacancy index processing failed")
            
        import_result = import_vacancy_main()
        if import_result != 0:
            raise Exception("Vacancy index import failed")
            
        # Update the database views
        update_database_views(config)
        
        logger.info("Daily update completed successfully")
        
    except Exception as e:
        logger.error(f"Daily update failed: {e}")
        # 不抛出异常，让调度器继续运行

@app.route('/')
def home():
    """Health check endpoint."""
    return {
        'status': 'healthy',
        'last_check': datetime.now().isoformat(),
        'service': 'real-analytics-db'
    }

@app.route('/run-update')
def trigger_update():
    """Manual trigger endpoint."""
    try:
        run_daily_update()
        return {'status': 'success', 'message': 'Update completed'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}, 500

def main():
    """Main entry point for the scheduler."""
    try:
        # 创建调度器
        scheduler = BlockingScheduler()
        
        # 设置每天凌晨2点（美国东部时间）运行
        scheduler.add_job(
            run_daily_update,
            trigger=CronTrigger(hour=2, minute=0, timezone='America/New_York'),
            id='daily_update',
            name='Daily Apartment List Data Update',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600  # 1 hour grace time
        )
        
        logger.info("Scheduler started, waiting for jobs...")
        
        # 在Render上，我们需要同时运行Flask和调度器
        port = int(os.environ.get('PORT', 5000))
        
        # 在单独的线程中启动调度器
        from threading import Thread
        scheduler_thread = Thread(target=scheduler.start, daemon=True)
        scheduler_thread.start()
        
        # 启动Flask应用
        app.run(host='0.0.0.0', port=port)
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        raise

if __name__ == "__main__":
    main() 