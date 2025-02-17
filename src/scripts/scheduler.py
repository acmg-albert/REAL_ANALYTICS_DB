"""Scheduler for automated data updates."""

import logging
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from ..database import SupabaseClient
from ..utils.config import Config
from .scrape_rent_estimates import main as scrape_main
from .process_rent_estimates import main as process_main
from .import_rent_estimates import main as import_main

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Path("logs") / "scheduler.log")
    ]
)

logger = logging.getLogger(__name__)

def update_database_view(config: Config):
    """Update the database view for rent estimates."""
    try:
        db = SupabaseClient(config.supabase_url, config.supabase_key)
        
        # SQL to refresh the materialized view
        sql = """
        REFRESH MATERIALIZED VIEW DB_VIEW_Apartment_List_Rent_Estimates_1_3;
        """
        
        # Execute the SQL
        response = db.execute_sql(sql)
        logger.info("Successfully updated database view")
        
    except Exception as e:
        logger.error(f"Failed to update database view: {e}")
        raise

def run_daily_update():
    """Run the daily update process."""
    try:
        logger.info("Starting daily update process")
        
        # Load configuration
        config = Config.from_env()
        
        # Run the update pipeline
        scrape_result = scrape_main()
        if scrape_result != 0:
            raise Exception("Scraping failed")
            
        process_result = process_main()
        if process_result != 0:
            raise Exception("Processing failed")
            
        import_result = import_main()
        if import_result != 0:
            raise Exception("Import failed")
            
        # Update the database view
        update_database_view(config)
        
        logger.info("Daily update completed successfully")
        
    except Exception as e:
        logger.error(f"Daily update failed: {e}")
        # 不抛出异常，让调度器继续运行

def main():
    """Main entry point for the scheduler."""
    try:
        scheduler = BlockingScheduler()
        
        # Schedule the job to run daily at 2 AM
        scheduler.add_job(
            run_daily_update,
            trigger=CronTrigger(hour=2, minute=0),
            id='daily_update',
            name='Daily Apartment List Data Update',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=3600  # 1 hour grace time
        )
        
        logger.info("Scheduler started, waiting for jobs...")
        scheduler.start()
        
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped")
    except Exception as e:
        logger.error(f"Scheduler error: {e}")
        raise

if __name__ == "__main__":
    main() 