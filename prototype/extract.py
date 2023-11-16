import os
import requests
import time
import json
import logging

# Environment variables
API_URL = os.environ.get("API_URL")
API_KEY = os.environ.get("API_KEY")

# Set the maximum number of items per page
LIMIT = 100

# Output folder
DATA_FOLDER = "data/"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Function to handle API requests with exponential backoff
def make_request(method, after=None):
    url = f"{API_URL}/{method}"
    params = {"limit": LIMIT, "apiKey": API_KEY}
    if after:
        params["starting_after"] = after

    response = requests.get(url, params=params)
    
    # Handle rate limiting (429 status code)
    while response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 1))
        logger.warning(f"Rate limited. Retrying in {retry_after} seconds for method: {method}")
        time.sleep(retry_after)
        response = requests.get(url, params=params)

    if response.status_code == 200:
        return response.json()
    elif response.status_code == 400:
        logger.warning(f"Bad Request for method: {method}. Status Code: {response.status_code}, Message: {response.text}")
        return None
    else:
        logger.error(f"Failed to fetch data for method: {method}. Status Code: {response.status_code}")
        return None

# Function to save data to a JSONL file
def save_data_to_file(data, method):
    with open(os.path.join(DATA_FOLDER, f"{method}.jsonl"), "a") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")

# Main function
def main():
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)

    import sys
    if len(sys.argv) < 2:
        logger.error("Usage: extract.py <method>... <method>")
        return

    methods = sys.argv[1:]
    
    for method in methods:
        after = None
        page_counter = 1
        while True:
            logger.info(f"Extracting page {page_counter} for method {method}")
            response = make_request(method, after)
            if response is None:
                break
            data = response.get("data", [])
            
            if not response['has_more']:
                logger.info("No more data available for method: {method}")
                break

            if not data:
                logger.info(f"No more data available for method: {method}")
                break

            save_data_to_file(data, method)

            # Set the cursor for the next page
            after = data[-1].get("id")
            page_counter += 1

if __name__ == "__main__":
    main()

