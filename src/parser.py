import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from bs4 import BeautifulSoup, Tag

from .utils import clean_text, parse_date

logger = logging.getLogger(__name__)

class MaruGujaratParser:
    """
    Parser for MaruGujarat website content
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the parser
        
        Args:
            config (Dict[str, Any]): Configuration dictionary
        """
        self.config = config
        self.parser_config = config.get('parser', {}).get('job_listings', {})
        logger.info("Initializing MaruGujaratParser")
    
    def parse_job_listing(self, html_content: str, url: str) -> Optional[Dict[str, Any]]:
        """
        Parse job listing HTML content
        
        Args:
            html_content (str): HTML content
            url (str): URL of the job listing
        
        Returns:
            Optional[Dict[str, Any]]: Parsed job details or None if parsing failed
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            
            # Extract basic job details
            title = self._extract_title(soup)
            date_info = self._extract_date(soup)
            description = self._extract_description(soup)
            apply_info = self._extract_apply_info(soup)
            category = self._extract_category(soup)
            
            # Extract structured job information
            job_info = self._extract_job_details(soup)
            
            # Combine all information
            job_details = {
                'url': url,
                'title': title,
                'posted_date': date_info.get('posted_date'),
                'last_date': date_info.get('last_date'),
                'description': description,
                'apply_url': apply_info.get('apply_url'),
                'application_method': apply_info.get('method'),
                'category': category,
                **job_info
            }
            
            return job_details
        
        except Exception as e:
            logger.error(f"Error parsing job listing from {url}: {e}")
            return None
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """
        Extract job title from soup
        
        Args:
            soup (BeautifulSoup): Parsed HTML
        
        Returns:
            str: Job title
        """
        title_selector = self.parser_config.get('title_selector', 'h1.entry-title')
        title_elem = soup.select_one(title_selector)
        
        if title_elem:
            return clean_text(title_elem.text)
        else:
            logger.warning("Could not find job title, using fallback")
            # Fallback to any heading that might contain the title
            for heading in soup.find_all(['h1', 'h2']):
                if heading.text.strip():
                    return clean_text(heading.text)
        
        return "Unknown Title"
    
    def _extract_date(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract dates from soup
        
        Args:
            soup (BeautifulSoup): Parsed HTML
        
        Returns:
            Dict[str, Any]: Dictionary with date information
        """
        date_info = {
            'posted_date': None,
            'last_date': None
        }
        
        date_selector = self.parser_config.get('date_selector', 'span.meta-date')
        date_elem = soup.select_one(date_selector)
        
        if date_elem:
            date_info['posted_date'] = clean_text(date_elem.text)
        
        # Look for last date to apply in the content
        last_date_patterns = [
            r'Last Date(?:\s+to Apply)?[:\s]+([0-9]{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+,?\s+[0-9]{4})',
            r'Last Date[:\s]+([0-9]{1,2}-[0-9]{1,2}-[0-9]{4})',
            r'Apply Before[:\s]+([0-9]{1,2}(?:st|nd|rd|th)?\s+[A-Za-z]+,?\s+[0-9]{4})'
        ]
        
        description_text = self._extract_description(soup)
        for pattern in last_date_patterns:
            match = re.search(pattern, description_text)
            if match:
                date_info['last_date'] = match.group(1)
                break
        
        return date_info
    
    def _extract_description(self, soup: BeautifulSoup) -> str:
        """
        Extract job description from soup
        
        Args:
            soup (BeautifulSoup): Parsed HTML
        
        Returns:
            str: Job description
        """
        description_selector = self.parser_config.get('description_selector', 'div.entry-content')
        description_elem = soup.select_one(description_selector)
        
        if description_elem:
            return clean_text(description_elem.text)
        else:
            logger.warning("Could not find job description")
            return "No description available"
    
    def _extract_apply_info(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract application information from soup
        
        Args:
            soup (BeautifulSoup): Parsed HTML
        
        Returns:
            Dict[str, Any]: Dictionary with application information
        """
        apply_info = {
            'apply_url': None,
            'method': None
        }
        
        apply_link_selector = self.parser_config.get('apply_link_selector', 'a.apply-button')
        apply_link = soup.select_one(apply_link_selector)
        
        if apply_link:
            apply_info['apply_url'] = apply_link.get('href')
            apply_info['method'] = 'online'
        else:
            # Look for application method in content
            description_text = self._extract_description(soup)
            
            if re.search(r'apply online', description_text, re.IGNORECASE):
                apply_info['method'] = 'online'
            elif re.search(r'apply offline', description_text, re.IGNORECASE):
                apply_info['method'] = 'offline'
            else:
                apply_info['method'] = 'unknown'
        
        return apply_info
    
    def _extract_category(self, soup: BeautifulSoup) -> str:
        """
        Extract job category from soup
        
        Args:
            soup (BeautifulSoup): Parsed HTML
        
        Returns:
            str: Job category
        """
        category_selector = self.parser_config.get('category_selector', 'span.category')
        category_elem = soup.select_one(category_selector)
        
        if category_elem:
            return clean_text(category_elem.text)
        else:
            # Try to infer category from title or content
            title = self._extract_title(soup)
            
            categories = {
                'government': ['government', 'sarkari', 'govt'],
                'education': ['education', 'school', 'university', 'college'],
                'medical': ['medical', 'hospital', 'healthcare', 'doctor', 'nurse'],
                'engineering': ['engineering', 'engineer', 'technical'],
                'banking': ['bank', 'banking', 'finance']
            }
            
            for category, keywords in categories.items():
                if any(keyword.lower() in title.lower() for keyword in keywords):
                    return category
            
            return "Uncategorized"
    
    def _extract_job_details(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """
        Extract detailed job information from soup
        
        Args:
            soup (BeautifulSoup): Parsed HTML
        
        Returns:
            Dict[str, Any]: Dictionary with job details
        """
        job_info = {
            'organization': None,
            'location': None,
            'salary': None,
            'vacancies': None,
            'qualification': None,
            'experience': None,
            'job_type': None,
            'important_dates': [],
            'important_links': []
        }
        
        # Extract from description
        description_text = self._extract_description(soup)
        
        # Organization name
        org_patterns = [
            r'Organization(?:\s+Name)?[:\s]+([^\n]+)',
            r'Department[:\s]+([^\n]+)',
            r'Company[:\s]+([^\n]+)'
        ]
        
        for pattern in org_patterns:
            match = re.search(pattern, description_text)
            if match:
                job_info['organization'] = clean_text(match.group(1))
                break
        
        # Location
        location_patterns = [
            r'Location[:\s]+([^\n]+)',
            r'Place[:\s]+([^\n]+)',
            r'Job Location[:\s]+([^\n]+)'
        ]
        
        for pattern in location_patterns:
            match = re.search(pattern, description_text)
            if match:
                job_info['location'] = clean_text(match.group(1))
                break
        
        # Salary
        salary_patterns = [
            r'Salary[:\s]+([^\n]+)',
            r'Pay Scale[:\s]+([^\n]+)',
            r'Stipend[:\s]+([^\n]+)'
        ]
        
        for pattern in salary_patterns:
            match = re.search(pattern, description_text)
            if match:
                job_info['salary'] = clean_text(match.group(1))
                break
        
        # Vacancies
        vacancy_patterns = [
            r'Vacancies[:\s]+([0-9]+)',
            r'Total Posts[:\s]+([0-9]+)',
            r'No\. of Posts[:\s]+([0-9]+)'
        ]
        
        for pattern in vacancy_patterns:
            match = re.search(pattern, description_text)
            if match:
                try:
                    job_info['vacancies'] = int(match.group(1))
                except ValueError:
                    job_info['vacancies'] = match.group(1)
                break
        
        # Qualification
        qualification_patterns = [
            r'Qualification[:\s]+([^\n]+)',
            r'Educational Qualification[:\s]+([^\n]+)',
            r'Education[:\s]+([^\n]+)'
        ]
        
        for pattern in qualification_patterns:
            match = re.search(pattern, description_text)
            if match:
                job_info['qualification'] = clean_text(match.group(1))
                break
        
        # Experience
        experience_patterns = [
            r'Experience[:\s]+([^\n]+)',
            r'Work Experience[:\s]+([^\n]+)',
            r'Required Experience[:\s]+([^\n]+)'
        ]
        
        for pattern in experience_patterns:
            match = re.search(pattern, description_text)
            if match:
                job_info['experience'] = clean_text(match.group(1))
                break
        
        # Job Type
        job_type_patterns = [
            r'Job Type[:\s]+([^\n]+)',
            r'Employment Type[:\s]+([^\n]+)',
            r'Type of Job[:\s]+([^\n]+)'
        ]
        
        for pattern in job_type_patterns:
            match = re.search(pattern, description_text)
            if match:
                job_info['job_type'] = clean_text(match.group(1))
                break
        
        # Important dates
        date_table = soup.find('table')
        if date_table:
            for row in date_table.find_all('tr'):
                cols = row.find_all('td')
                if len(cols) >= 2:
                    date_event = clean_text(cols[0].text)
                    date_value = clean_text(cols[1].text)
                    job_info['important_dates'].append({
                        'event': date_event,
                        'date': date_value
                    })
        
        # Important links
        link_patterns = [
            r'Official Website[:\s]+([^\n]+)',
            r'Notification[:\s]+([^\n]+)',
            r'Apply Online[:\s]+([^\n]+)'
        ]
        
        for pattern in link_patterns:
            match = re.search(pattern, description_text)
            if match:
                link_text = clean_text(match.group(1))
                if re.match(r'https?://', link_text):
                    job_info['important_links'].append({
                        'type': pattern.split('[')[0].strip(),
                        'url': link_text
                    })
        
        # Also extract links from the content
        for link in soup.select('a[href^="http"]'):
            href = link.get('href')
            text = clean_text(link.text)
            if href and text and len(text) > 3:  # Avoid empty or very short link texts
                job_info['important_links'].append({
                    'type': text,
                    'url': href
                })
        
        return job_info