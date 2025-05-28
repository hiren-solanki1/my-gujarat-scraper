import logging
import os
import json
import re
from typing import Dict, Any, List, Set
from bs4 import BeautifulSoup
from tqdm import tqdm

from .utils import make_request

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

        # Get whitelist and blacklist from config
        default_whitelist = [
            "GPSC", "GSSSB", "Talati", "TET", "HTAT", "TAT", "Clerk", "PSI", "Police",
            "Constable", "Gujarat", "OJAS", "High Court", "HC", "LRD", "Bharti",
            "Exam",
        ]
        self.whitelist_keywords = config.get('whitelist_keywords', default_whitelist)
        self.whitelist_keywords = [kw.lower() for kw in self.whitelist_keywords]

        default_blacklist = [
            "University", "Apprentice", "Apprenticeship", "Contract", "Outsourcing",
            "District Project Coordinator", "Project Coordinator", "Project",
            "Contractual", "Operator", "Walk-in Interview", "Rozgaar Bharti Melo",
            "Consultant", "CSIR", "CSMCRI", "Samagra Shiksha", "College", "Hospital",
            "IRMA", "TB", "GMERS", "GNLU", "Shikshan Sahayak", "Nagarpalika",
            "Part-Time", "Technician", "Paper"
        ]
        self.blacklist_keywords = config.get('blacklist_keywords', default_blacklist)
        self.blacklist_keywords = [kw.lower() for kw in self.blacklist_keywords]

        self.existing_jobs = self._load_existing_notification()

        logger.info(f"Initializing MaruGujaratScraper with base URL: {self.base_url}")
        logger.info(f"Loaded {len(self.existing_jobs)} existing job titles for duplicate checking")
        logger.info(f"Using {len(self.whitelist_keywords)} whitelist keywords and {len(self.blacklist_keywords)} blacklist keywords")

    def clean_title(self, title: str) -> str:
        """
        Clean and rephrase notification title according to rules:
        1. Remove emojis
        2. Remove parentheses and text inside
        3. Remove text after symbols like -, :, |

        Args:
            title (str): Original title

        Returns:
            str: Cleaned title
        """
        title = re.sub(r'[^\x00-\x7F]+', '', title)
        title = re.sub(r'\([^)]*\)', '', title)
        title = re.split(r'[-:|]', title)[0]
        title = re.sub(r'\s+', ' ', title).strip()
        return title

    def _load_existing_notification(self) -> Set[str]:
        """
        Load existing notification titles and links to prevent duplicates

        Returns:
            Set[str]: Set of existing notification titles and links
        """
        existing_notifications = set()
        data_dir = self.config.get('storage', {}).get('directory', 'data/processed')

        if not os.path.exists(data_dir):
            return existing_notifications

        for filename in os.listdir(data_dir):
            if filename.endswith('.json'):
                try:
                    file_path = os.path.join(data_dir, filename)
                    with open(file_path, 'r', encoding='utf-8') as f:
                        jobs = json.load(f)
                        for job in jobs:
                            existing_notifications.add(job.get('title', '').lower())
                            existing_notifications.add(job.get('link', '').lower())
                except Exception as e:
                    logger.warning(f"Error loading existing jobs from {filename}: {e}")

        return existing_notifications
    
    @staticmethod
    def extract_apply_online_link(html_content):
        soup = BeautifulSoup(html_content, 'html.parser')

        rows = soup.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 2:
                description = cols[0].get_text(strip=True)
                if "Apply Online" in description:
                    link_tag = cols[1].find('a', href=True)
                    if link_tag:
                        return link_tag['href']

        for row in rows:
            cols = row.find_all('td')
            if len(cols) == 2:
                description = cols[0].get_text(strip=True)
                if "Official Portal" in description:
                    link_tag = cols[1].find('a', href=True)
                    if link_tag:
                        return link_tag['href']

        return None

    def get_notification_data(self, link: str) -> Dict[str, Any]:
        logger.info(f"Fetching notification data from {link}")

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

        # Use make_request to fetch the page
        response = make_request(
            link, self.config, method="POST", headers=headers, data={}
        )

        if not response:
            logger.error(f"Failed to get notification from link {link}")
            return {}

        soup = BeautifulSoup(response.content, "lxml")
        publish_date_tag = soup.select_one("time.entry-date.published")
        return {
            "publish_date": publish_date_tag.get_text(strip=True) if publish_date_tag else "N/A",
            "apply_online_link": self.extract_apply_online_link(response.content),
        }


    def get_notifications_listings_updates(self, page_number: int):
        logger.info(f"Fetching notification listings from page {page_number}")

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
            "page": f"{page_number}",
            "lang": "",
            "ajax_nonce": "7b50977e00",
            "custom_data[sf_taxo]": "{}",
            "custom_data[sf_opera]": "{}",
        }

        url = "https://www.marugujarat.in/wp-admin/admin-ajax.php"

        response = make_request(
            url, self.config, method="POST", headers=headers, data=data
        )

        if not response:
            logger.error(f"Failed to get notification listings from page {page_number}")
            return []

        soup = BeautifulSoup(response.content, "lxml")

        results = []

        for tag in soup.select("h4.pt-cv-title a"):
            original_title = tag.get_text(strip=True)
            lower_title = original_title.lower()
            link = tag["href"]

            if lower_title in self.existing_jobs or link.lower() in self.existing_jobs:
                logger.debug(f"Skipping duplicate job: {original_title}")
                continue

            contains_whitelist = any(white_kw in lower_title for white_kw in self.whitelist_keywords)
            not_contains_blacklist = not any(black_kw in lower_title for black_kw in self.blacklist_keywords)

            if contains_whitelist and not_contains_blacklist:
                cleaned_title = self.clean_title(original_title)

                results.append({
                    "title": cleaned_title,
                    "original_title": original_title,  # Keep original for reference
                    "link": link,
                })

                self.existing_jobs.add(lower_title)
                self.existing_jobs.add(link.lower())

        logger.info(f"Found {len(results)} new job listings on page {page_number}")
        return results

    def scrape_all_notifications(self) -> List[Dict[str, Any]]:
        """
        Scrape all notification listings with randomized delays between page requests

        Returns:
            List[Dict[str, Any]]: List of notifications
        """
        logger.info("Starting to scrape all notifications")
        total_pages = self.config.get('pages_to_scrape', 3)
        logger.info(f"Found {total_pages} pages to scrape")

        all_notifications = []
        for page in tqdm(range(1, total_pages + 1), desc="Collecting notifications Pages Links"):
            notifications_data = self.get_notifications_listings_updates(page)
            all_notifications.extend(notifications_data)


        notifications = []
        for notification in tqdm(all_notifications, desc="Processing notification updates"):
            link = notification.get("link")
            data = self.get_notification_data(link)
            if data:
                small_dir = {
                    "title": notification.get("title"),
                    "page_link": link,
                    "publish_date": data.get("publish_date", "N/A"),
                    "apply_online_link": data.get("apply_online_link", ""),
                }
                notifications.append(small_dir)

        logger.info(f"Successfully scraped {len(notifications)} notifications")

        return notifications