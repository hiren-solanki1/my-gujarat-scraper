import time
import random
import logging
import requests
from typing import Dict, Any, Optional
from fake_useragent import UserAgent

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

def make_request(
    url: str,
    config: Dict[str, Any],
    method: str = "GET",
    params: Optional[Dict[str, Any]] = None,
    data: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, Any]] = None,
    json: Optional[Dict[str, Any]] = None,
) -> Optional[requests.Response]:
    """
    Make an HTTP request with retry logic and rate limiting

    Args:
        url (str): URL to request
        config (Dict[str, Any]): Configuration dictionary
        method (str): HTTP method (GET, POST, etc.)
        params (Dict[str, Any], optional): Request parameters. Defaults to None.
        data (Dict[str, Any], optional): Form data for POST requests. Defaults to None.
        headers (Dict[str, Any], optional): Custom headers. Defaults to None.
        json (Dict[str, Any], optional): JSON data for POST requests. Defaults to None.

    Returns:
        Optional[requests.Response]: Response object or None if failed
    """
    scraper_config = config.get("scraper", {})
    timeout = scraper_config.get("request_timeout", 30)
    max_retries = scraper_config.get("max_retries", 3)
    retry_delay = scraper_config.get("retry_delay", 5)

    default_headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
    }

    req_headers = headers if headers else default_headers

    if (
        scraper_config.get("user_agent_rotation", True)
        and "User-Agent" not in req_headers
    ):
        req_headers["User-Agent"] = get_random_user_agent()

    # Rate limiting
    if "rate_limit" in scraper_config:
        rate_limit = scraper_config["rate_limit"].get("requests_per_minute", 10)
        time.sleep(60 / rate_limit)

    # Retry logic
    for attempt in range(max_retries):
        try:
            if method.upper() == "GET":
                response = requests.get(
                    url, headers=req_headers, params=params, timeout=timeout
                )
            elif method.upper() == "POST":
                response = requests.post(
                    url,
                    headers=req_headers,
                    params=params,
                    data=data,
                    json=json,
                    timeout=timeout,
                )
            else:
                response = requests.request(
                    method,
                    url,
                    headers=req_headers,
                    params=params,
                    data=data,
                    json=json,
                    timeout=timeout,
                )

            response.raise_for_status()
            return response
        except requests.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                # Add jitter to retry delay
                jitter = random.uniform(0.1, 1.0)
                sleep_time = retry_delay + jitter
                logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                logger.error(f"Max retries reached for URL: {url}")
                return None
