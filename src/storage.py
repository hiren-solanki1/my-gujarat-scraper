import os
import json
import csv
import logging
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime
import sqlite3

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
        self.filename_prefix = self.storage_config.get('filename_prefix', 'marugujarat_jobs')
        
        # Create directory if it doesn't exist
        os.makedirs(self.directory, exist_ok=True)
        
        logger.info(f"Initialized storage with format: {self.format}")
    
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
        elif self.format.lower() == 'sqlite':
            return self._save_sqlite(jobs, timestamp)
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
            # Flatten nested dictionaries
            flattened_jobs = []
            for job in jobs:
                flat_job = {}
                for key, value in job.items():
                    if isinstance(value, dict):
                        for sub_key, sub_value in value.items():
                            flat_job[f"{key}_{sub_key}"] = sub_value
                    elif isinstance(value, list):
                        flat_job[key] = json.dumps(value)
                    else:
                        flat_job[key] = value
                flattened_jobs.append(flat_job)
            
            # Convert to DataFrame and save
            df = pd.DataFrame(flattened_jobs)
            df.to_csv(filepath, index=False, encoding='utf-8')
            
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
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(jobs, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(jobs)} jobs to JSON: {filepath}")
            return filepath
        
        except Exception as e:
            logger.error(f"Error saving to JSON: {e}")
            return ""
    
    def _save_sqlite(self, jobs: List[Dict[str, Any]], timestamp: str) -> str:
        """
        Save job listings to SQLite database
        
        Args:
            jobs (List[Dict[str, Any]]): List of job details
            timestamp (str): Timestamp string
        
        Returns:
            str: Path to the saved file
        """
        filename = f"{self.filename_prefix}.db"
        filepath = os.path.join(self.directory, filename)
        
        try:
            conn = sqlite3.connect(filepath)
            
            # Create tables if they don't exist
            conn.execute('''
            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                title TEXT,
                posted_date TEXT,
                last_date TEXT,
                description TEXT,
                apply_url TEXT,
                application_method TEXT,
                category TEXT,
                organization TEXT,
                location TEXT,
                salary TEXT,
                vacancies TEXT,
                qualification TEXT,
                experience TEXT,
                job_type TEXT,
                scraped_at TEXT
            )
            ''')
            
            conn.execute('''
            CREATE TABLE IF NOT EXISTS important_dates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER,
                event TEXT,
                date TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs (id)
            )
            ''')
            
            conn.execute('''
            CREATE TABLE IF NOT EXISTS important_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER,
                type TEXT,
                url TEXT,
                FOREIGN KEY (job_id) REFERENCES jobs (id)
            )
            ''')
            
            # Insert jobs
            for job in jobs:
                # Extract fields for jobs table
                job_data = {
                    'url': job.get('url'),
                    'title': job.get('title'),
                    'posted_date': job.get('posted_date'),
                    'last_date': job.get('last_date'),
                    'description': job.get('description'),
                    'apply_url': job.get('apply_url'),
                    'application_method': job.get('application_method'),
                    'category': job.get('category'),
                    'organization': job.get('organization'),
                    'location': job.get('location'),
                    'salary': job.get('salary'),
                    'vacancies': str(job.get('vacancies')),
                    'qualification': job.get('qualification'),
                    'experience': job.get('experience'),
                    'job_type': job.get('job_type'),
                    'scraped_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                
                # Insert job and get ID
                cursor = conn.cursor()
                
                # Check if job already exists
                cursor.execute('SELECT id FROM jobs WHERE url = ?', (job_data['url'],))
                existing_job = cursor.fetchone()
                
                if existing_job:
                    job_id = existing_job[0]
                    # Update existing job
                    placeholders = ', '.join([f"{k} = ?" for k in job_data.keys() if k != 'url'])
                    values = [v for k, v in job_data.items() if k != 'url']
                    values.append(job_data['url'])
                    
                    cursor.execute(f"UPDATE jobs SET {placeholders} WHERE url = ?", values)
                else:
                    # Insert new job
                    placeholders = ', '.join(['?'] * len(job_data))
                    columns = ', '.join(job_data.keys())
                    
                    cursor.execute(f"INSERT INTO jobs ({columns}) VALUES ({placeholders})", list(job_data.values()))
                    job_id = cursor.lastrowid
                
                # Insert important dates
                important_dates = job.get('important_dates', [])
                for date_entry in important_dates:
                    cursor.execute(
                        "INSERT INTO important_dates (job_id, event, date) VALUES (?, ?, ?)",
                        (job_id, date_entry.get('event'), date_entry.get('date'))
                    )
                
                # Insert important links
                important_links = job.get('important_links', [])
                for link_entry in important_links:
                    cursor.execute(
                        "INSERT INTO important_links (job_id, type, url) VALUES (?, ?, ?)",
                        (job_id, link_entry.get('type'), link_entry.get('url'))
                    )
            
            conn.commit()
            conn.close()
            
            logger.info(f"Saved {len(jobs)} jobs to SQLite database: {filepath}")
            return filepath
        
        except Exception as e:
            logger.error(f"Error saving to SQLite: {e}")
            return ""