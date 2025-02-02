import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# MySQL Configuration
MYSQL_CONFIG = {
    'host': os.getenv('SQL_HOST', 'localhost'),
    'user': os.getenv('SQL_USERNAME', 'root'),
    'password': os.getenv('SQL_PASSWORD', ''),
    'database': os.getenv('SQL_DATABASE', 'test'),
    'port': int(os.getenv('SQL_PORT', 3306))
}

# Base logs directory
BASE_LOG_DIR = "logs"
