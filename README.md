# MaruGujarat Scraper

A Python web scraper for extracting job listings from the MaruGujarat website.

## Features

- Scrapes job listings from MaruGujarat
- Extracts detailed job information including titles, descriptions, and application details
- Handles pagination
- Respects rate limits and robots.txt
- Provides multiple storage options (CSV, JSON, SQLite)
- Configurable via YAML file

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/maru-gujarat-scraper.git
   cd maru-gujarat-scraper
   ```

## Project Structure

```
maru-gujarat-scraper/
├── src/
│   ├── __init__.py
│   ├── scraper.py     # Main scraping functionality
│   ├── parser.py      # HTML parsing functionality
│   ├── storage.py     # Data storage functionality
│   ├── utils.py       # Utility functions
│   └── config/
│       └── __init__.py  # Configuration loading
├── data/
│   ├── raw/           # Raw downloaded HTML
│   └── processed/     # Processed data output
├── tests/
│   ├── __init__.py
│   ├── test_scraper.py
│   └── test_parser.py
├── logs/              # Log files
├── requirements.txt   # Dependencies
├── main.py            # Entry point
├── config.yaml        # Configuration
└── README.md          # Documentation
```
