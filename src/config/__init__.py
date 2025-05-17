import os
import yaml
from typing import Dict, Any
import logging

def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file
    
    Args:
        config_path (str, optional): Path to config file. Defaults to project root config.yaml.
    
    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    if config_path is None:
        # Try to find the config file in the project root
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(base_dir, 'config.yaml')
    
    try:
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        return config
    except FileNotFoundError:
        logging.error(f"Config file not found at {config_path}")
        raise
    except yaml.YAMLError as e:
        logging.error(f"Error parsing config file: {e}")
        raise

def setup_logging(config: Dict[str, Any] = None) -> None:
    """
    Setup logging based on configuration
    
    Args:
        config (Dict[str, Any], optional): Configuration dictionary. Defaults to None.
    """
    if config is None:
        config = load_config()
    
    log_config = config.get('logging', {})
    log_level = getattr(logging, log_config.get('level', 'INFO'))
    log_format = log_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_file = log_config.get('file', 'logs/scraper.log')
    
    # Create log directory if it doesn't exist
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )