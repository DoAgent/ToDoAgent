# config_loader.py
import yaml
from pathlib import Path

def load_config():
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

CONFIG = load_config()

def get_mysql_config():
    return {
        **CONFIG['mysql'],
        'ssl_ca': str(Path(__file__).parent / CONFIG['mysql']['ssl_ca'])
    }

def get_openai_config():
    return CONFIG['openai']

def get_paths():
    return CONFIG['paths']