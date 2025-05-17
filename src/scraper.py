import logging
import time
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

from .utils import make_request, normalize_url

logger = logging.getLogger(__name__)

class MaruGujaratScraper:
    """
    Scraper for MaruGujarat website
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the scraper
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.base_url = config.get('base_url', 'https://www.marugujarat.in/maru-gujarat/')
        logger.info(f"Initializing MaruGujaratScraper with base URL: {self.base_url}")


    def get_page_count(self) -> int:
        """
        Get the total number of pages to scrape
        
        Returns:
            int: Total number of pages
        """
        try:
            response = make_request(self.base_url, self.config)
            if not response:
                logger.error("Failed to get page count")
                return 1
            
            soup = BeautifulSoup(response.content, 'lxml')
            
            # Look for pagination elements
            pagination = soup.select('.pagination a')
            if not pagination:
                logger.warning("No pagination found, assuming single page")
                return 1
            
            # Try to find the last page number
            page_numbers = []
            for page_link in pagination:
                try:
                    page_num = int(page_link.text.strip())
                    page_numbers.append(page_num)
                except ValueError:
                    # Skip non-numeric pagination elements
                    continue
            
            if page_numbers:
                return max(page_numbers)
            else:
                logger.warning("Could not determine page count, assuming single page")
                return 1
        
        except Exception as e:
            logger.error(f"Error getting page count: {e}")
            return 1
    
    def get_job_listings_updates(self, page_number: int):
        logger.info(f"Fetching job listings from page {page_number}")

        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "origin": "https://www.marugujarat.in",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "https://www.marugujarat.in/maru-gujarat/?_page=77",
            "sec-ch-ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }

        data = {
            "action": "pagination_request",
            "sid": "f2c0a65lf2",
            "unid": "",
            "isblock": "",
            "postid": "",
            "page": f'{page_number}',
            "lang": "",
            "ajax_nonce": "7b50977e00",
            "custom_data[sf_taxo]": "{}",
            "custom_data[sf_opera]": "{}",
        }

        response = requests.post(
            "https://www.marugujarat.in/wp-admin/admin-ajax.php",
            headers=headers,
            data=data,
        )


        if not response:
            logger.error(f"Failed to get job listings from page {page_number}")
            return []
        

        blacklist_keywords = [
            "University",
            "Apprentice",
            "Apprenticeship",
            "Contract",
            "Outsourcing",
            "District Project Coordinator",
            "Project Coordinator",
            "Project",
            "Contractual",
            "Operator",
            "Walk-in Interview",
            "Rozgaar Bharti Melo",
            "Consultant",
            "CSIR",
            "CSMCRI",
            "Samagra Shiksha",
            "College",
            "Hospital",
            "IRMA",
            "TB",
            "GMERS",
            "GNLU",
            "Shikshan Sahayak",
            "Nagarpalika",
            "Part-Time",
            "Technician",
            "Paper",
        ]

        blacklist_keywords = [kw.lower() for kw in blacklist_keywords]

        soup = BeautifulSoup(response.content, 'lxml')

        results = []

        for tag in soup.select("h4.pt-cv-title a"):
            title = tag.get_text(strip=True)
            lower_title = title.lower()

            if not any(black_kw in lower_title for black_kw in blacklist_keywords):
                results.append({
                    "title": title,
                    "link": tag["href"],
                })
        print('ress', results)
        return results

    def scrape_all_jobs(self) -> List[Dict[str, Any]]:
        """
        Scrape all job listings
        
        Returns:
            List[Dict[str, Any]]: List of job details
        """
        logger.info("Starting to scrape all jobs")
        
        total_pages = self.get_page_count()
        logger.info(f"Found {total_pages} pages to scrape")

        all_jobs = []
        for page in tqdm(range(1, total_pages + 1), desc="Collecting job Updates"):
            job_data = self.get_job_listings_updates(page)
            all_jobs.extend(job_data)
            time.sleep(2)

        logger.info(f"Successfully scraped {len(all_jobs)} job details")
        return all_jobs
