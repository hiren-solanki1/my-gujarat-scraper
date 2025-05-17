import time
import random
import logging
import requests
from typing import Dict, Any, Optional
from datetime import datetime
from fake_useragent import UserAgent
from urllib.parse import urljoin

logger = logging.getLogger(__name__)

def get_random_user_agent() -> str:
    """
    Get a random user agent string
    
    Returns:
        str: Random user agent string
    """
    try:
        ua = UserAgent()
        return ua.random
    except Exception as e:
        logger.warning(f"Failed to get random user agent: {e}")
        # Fallback to a common user agent
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

def make_request(url: str, config: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Optional[requests.Response]:
    """
    Make an HTTP request with retry logic and rate limiting
    
    Args:
        url (str): URL to request
        config (Dict[str, Any]): Configuration dictionary
        params (Dict[str, Any], optional): Request parameters. Defaults to None.
    
    Returns:
        Optional[requests.Response]: Response object or None if failed
    """
    scraper_config = config.get('scraper', {})
    timeout = scraper_config.get('request_timeout', 30)
    max_retries = scraper_config.get('max_retries', 3)
    retry_delay = scraper_config.get('retry_delay', 5)

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
    if scraper_config.get('user_agent_rotation', True):
        headers['User-Agent'] = get_random_user_agent()
    
    # Rate limiting
    if 'rate_limit' in scraper_config:
        rate_limit = scraper_config['rate_limit'].get('requests_per_minute', 10)
        # Simple rate limiting by sleeping
        time.sleep(60 / rate_limit)
    
    # Retry logic
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                # Add jitter to retry delay
                jitter = random.uniform(0.1, 1.0)
                sleep_time = retry_delay + jitter
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Max retries reached for URL: {url}")
                return None

def normalize_url(base_url: str, url: str) -> str:
    """
    Normalize URL by joining it with the base URL if it's relative
    
    Args:
        base_url (str): Base URL
        url (str): URL to normalize
    
    Returns:
        str: Normalized URL
    """
    return urljoin(base_url, url)

def parse_date(date_str: str) -> datetime:
    """
    Parse date string into datetime object
    
    Args:
        date_str (str): Date string
    
    Returns:
        datetime: Datetime object
    """
    try:
        # Try common formats
        formats = [
            "%d %B %Y",
            "%B %d, %Y",
            "%d-%m-%Y",
            "%Y-%m-%d",
            "%d/%m/%Y"
        ]
        
        for date_format in formats:
            try:
                return datetime.strptime(date_str.strip(), date_format)
            except ValueError:
                continue
        
        # If none of the formats match
        logger.warning(f"Could not parse date: {date_str}")
        return datetime.now()  # Default to current date
    except Exception as e:
        logger.error(f"Error parsing date '{date_str}': {e}")
        return datetime.now()

def clean_text(text: str) -> str:
    """
    Clean text by removing extra whitespace
    
    Args:
        text (str): Text to clean
    
    Returns:
        str: Cleaned text
    """
    if text is None:
        return ""
    return " ".join(text.strip().split())