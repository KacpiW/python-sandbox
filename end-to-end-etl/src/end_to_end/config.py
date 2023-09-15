import os
import logging
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv, find_dotenv

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path, override=True)


def setup_logger(logger_name: str, log_file: str) -> logging.Logger:
    """
    Set up a logger with the specified name, log file, and log level.
    """
    debug = os.getenv('DEBUG')
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG if debug else logging.INFO)

    handler = RotatingFileHandler(log_file, maxBytes=100000, backupCount=10)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# Global Variables and Sensitive Information from env
HOST = os.environ.get("HOST")  # replace "localhost" with your value
USER = os.environ.get("USER")  # replace "user" with your value
PASSWD = os.environ.get("PASSWD")  # replace "pass" with your value
DB = os.environ.get("DB")  # replace "database" with your value
# replace "bucket" with your value
EXTRACT_S3_BUCKET = os.environ.get("EXTRACT_S3_BUCKET")
# replace "bucket" with your value
LOAD_S3_BUCKET = os.environ.get("LOAD_S3_BUCKET")
# replace "file_name" with your value
LAST_PROCESSED_FILE_NAME = os.environ.get("LAST_PROCESSED_FILE_NAME")
