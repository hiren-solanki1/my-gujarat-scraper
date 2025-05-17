import argparse
import logging
import sys
import time
from typing import Dict, Any

from src.config import load_config, setup_logging
from src.scraper import MaruGujaratScraper
from src.storage import MaruGujaratStorage

def parse_args():
    """
    Parse command line arguments
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='MaruGujarat Scraper')
    parser.add_argument('--config', type=str, help='Path to config file')
    parser.add_argument('--format', type=str, choices=['csv', 'json', 'sqlite'], 
                        help='Output format (overrides config)')
    parser.add_argument('--output', type=str, help='Output directory (overrides config)')
    return parser.parse_args()

def main():
    """
    Main entry point for the scraper
    """
    # Parse command line arguments
    args = parse_args()
    
    try:
        # Load configuration
        config = load_config(args.config)
        
        # Override config with command line arguments if provided
        if args.format:
            config['storage']['format'] = args.format
        if args.output:
            config['storage']['directory'] = args.output
        
        # Setup logging
        setup_logging(config)
        
        logger = logging.getLogger(__name__)
        logger.info("Starting MaruGujarat scraper")
        
        # Initialize scraper
        scraper = MaruGujaratScraper(config)
        
        # Scrape all jobs
        start_time = time.time()
        jobs = scraper.scrape_all_jobs()
        end_time = time.time()
        
        logger.info(f"Scraped {len(jobs)} jobs in {end_time - start_time:.2f} seconds")
        
        # Save results
        storage = MaruGujaratStorage(config)
        output_path = storage.save(jobs)
        
        if output_path:
            logger.info(f"Results saved to: {output_path}")
        else:
            logger.error("Failed to save results")
            return 1
        
        logger.info("Scraping completed successfully")
        return 0
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())