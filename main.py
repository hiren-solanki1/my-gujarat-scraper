#!/usr/bin/env python
"""
Scheduled runner for MaruGujarat scraper.
Runs the scraper immediately and then schedules it to run every 3 hours.
"""
import asyncio
import logging
import sys
from datetime import datetime

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.config import load_config, setup_logging
from src.scraper import MaruGujaratScraper
from src.storage import MaruGujaratStorage

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
scheduler_logger = logging.getLogger("scheduler")

async def run_scraper_job():
    """Run the MaruGujarat scraper and save results"""
    start_time = datetime.now()
    scheduler_logger.info(f"Starting scraper job at {start_time}")

    try:
        config = load_config("config.yaml")
        setup_logging(config)
        logger = logging.getLogger("scraper_job")

        scraper = MaruGujaratScraper(config)
        jobs = scraper.scrape_all_notifications()

        storage = MaruGujaratStorage(config)
        output_path = storage.save(jobs)

        if output_path:
            logger.info(f"Results saved to: {output_path}")
        else:
            logger.error("Failed to save results")

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        scheduler_logger.info(f"Scraper job completed in {duration:.2f} seconds, found {len(jobs)} jobs")

    except Exception as e:
        scheduler_logger.error(f"Error in scraper job: {e}", exc_info=True)

async def main():
    """Main function that sets up and runs the scheduler"""
    scheduler_logger.info("Starting MaruGujarat Scheduler")

    # Create a scheduler
    scheduler = AsyncIOScheduler()

    # Run the job immediately
    scheduler_logger.info("Running initial scraper job...")
    await run_scraper_job()

    # Schedule the job to run every 3 hours
    scheduler.add_job(
        run_scraper_job,
        trigger=IntervalTrigger(hours=3),
        id="scraper_job",
        name="MaruGujarat Scraper Job",
        replace_existing=True,
    )

    scheduler.start()
    scheduler_logger.info("Scheduler started - job will run every 3 hours")

    try:
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        scheduler_logger.info("Shutting down scheduler...")
        scheduler.shutdown()
        scheduler_logger.info("Scheduler stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        scheduler_logger.info("Process interrupted by user")
        sys.exit(0)