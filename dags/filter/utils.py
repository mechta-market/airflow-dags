import time
import os
import logging

import requests

# Constants

REQUEST_MAX_RETRIES=3
REQUEST_RETRY_DELAY=1
REQUEST_TIMEOUT=60

# Functions

def fetch_with_retry(url: str, params=None, retries=REQUEST_MAX_RETRIES):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if attempt < retries - 1:
                time.sleep(REQUEST_RETRY_DELAY * (attempt + 1))
            else:
                raise

def clean_tmp_file(file_path: str):
    if file_path == "" or file_path is None:
        return

    if os.path.exists(file_path):
        os.remove(file_path)
        logging.info(f"Temporary file removed: {file_path}")
    else:
        logging.info(f"File does not exist: {file_path}")