# MaruGujarat Scraper

A Python web scraper for extracting Notification from the MaruGujarat website.


## Configuration
The scraper is configured via the config.yaml file. You can customize various settings:


Customizing Whitelist and Blacklist Keywords
The scraper filters job listings based on whitelist (include) and blacklist (exclude) keywords:


Whitelist Keywords: Only include job listings that contain at least one of these keywords
Blacklist Keywords: Exclude job listings that contain any of these keywords
To modify these keywords, edit the config.yaml file:


## Installation

Create a virtual environment (recommended):
python -m venv venv
## On Windows
venv\Scripts\activate
## On macOS/Linux
source venv/bin/activate

## Install dependencies:

pip install -r requirements.txt


## Project Structure

```
maru-gujarat-scraper/
├── src/
│   ├── __init__.py
│   ├── scraper.py     # Main scraping functionality
│   ├── storage.py     # Data storage functionality
│   ├── utils.py       # Utility functions
│   └── config/
│       └── __init__.py  # Configuration loading
├── data/
│   └── processed/     # Processed data output
├── logs/              # Log files
├── requirements.txt   # Dependencies
├── main.py            # Entry point
├── config.yaml        # Configuration
└── README.md          # Documentation
```
