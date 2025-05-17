import os
import json
import logging
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime

logger = logging.getLogger(__name__)

class MaruGujaratStorage:
    """
    Storage handler for MaruGujarat scraped data
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the storage handler

        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.storage_config = config.get('storage', {})
        self.format = self.storage_config.get('format', 'csv')
        self.directory = self.storage_config.get('directory', 'data/processed')
        self.filename_prefix = self.storage_config.get('filename_prefix', 'marugujarat_updates')

        # Handle directory creation
        self._ensure_directory()

        logger.info(f"Initialized storage with format: {self.format}")

    def _ensure_directory(self):
        """Create directory path, handling the case where a file exists with that name"""
        try:
            if os.path.exists(self.directory) and not os.path.isdir(self.directory):
                os.remove(self.directory)
                logger.warning(f"Removed file with same name as directory: {self.directory}")

            os.makedirs(self.directory, exist_ok=True)
            logger.info(f"Storage directory ready: {self.directory}")
        except Exception as e:
            logger.error(f"Failed to create directory {self.directory}: {e}")
            self.directory = "."
            logger.info("Falling back to current directory")

    def save(self, jobs: List[Dict[str, Any]]) -> str:
        """
        Save job listings to storage

        Args:
            jobs (List[Dict[str, Any]]): List of job details

        Returns:
            str: Path to the saved file
        """
        if not jobs:
            logger.warning("No jobs to save")
            return ""

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if self.format.lower() == 'csv':
            return self._save_csv(jobs, timestamp)
        elif self.format.lower() == 'json':
            return self._save_json(jobs, timestamp)
        else:
            logger.error(f"Unsupported storage format: {self.format}")
            # Default to JSON
            return self._save_json(jobs, timestamp)

    def _save_csv(self, jobs: List[Dict[str, Any]], timestamp: str) -> str:
        """
        Save job listings to CSV

        Args:
            jobs (List[Dict[str, Any]]): List of job details
            timestamp (str): Timestamp string

        Returns:
            str: Path to the saved file
        """
        filename = f"{self.filename_prefix}_{timestamp}.csv"
        filepath = os.path.join(self.directory, filename)

        try:
            for job in jobs:
                for key, value in job.items():
                    if isinstance(value, list):
                        job[key] = str(value)

            df = pd.DataFrame(jobs)
            df.to_csv(filepath, index=False, encoding='utf-8', mode='w')

            logger.info(f"Saved {len(jobs)} jobs to CSV: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return ""

    def _save_json(self, jobs: List[Dict[str, Any]], timestamp: str) -> str:
        """
        Save job listings to JSON

        Args:
            jobs (List[Dict[str, Any]]): List of job details
            timestamp (str): Timestamp string

        Returns:
            str: Path to the saved file
        """
        filename = f"{self.filename_prefix}_{timestamp}.json"
        filepath = os.path.join(self.directory, filename)

        try:
            # Open file in write mode to overwrite if it exists
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(jobs, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved {len(jobs)} jobs to JSON: {filepath}")
            return filepath

        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            return ""