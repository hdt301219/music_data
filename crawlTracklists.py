import json
import re
import threading
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from db_connection import get_database_connection

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def parse_track_details(details):
    pattern = re.compile(r"([\w\s\(\)\/\u2117]+):\s*(.+)")
    matches = pattern.findall(details)
    return {match[0].strip(): match[1].strip() for match in matches}

def scrape_release_data(release_id, cache, semaphore):
    logger.debug(f"Starting to scrape data for release ID {release_id}")
    
    # Set up headless Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")  # Suppress non-critical logs

    driver = None
    with semaphore:
        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            url = f"https://musicbrainz.org/release/{release_id}"
            driver.get(url)

            track_elements = driver.find_elements(By.CSS_SELECTOR, 'table.tbl > tbody > tr')
            for track in track_elements:
                track_details = track.find_elements(By.TAG_NAME, 'td')
                if track_details:
                    full_title_text = track_details[1].text
                    title, *additional_details = full_title_text.split('\n', 1)

                    track_dict = {
                        'Number': track_details[0].text,
                        'Title': title.strip(),
                        'Length': track_details[2].text,
                        'Variation_id': release_id
                    }

                    if additional_details:
                        additional_info = parse_track_details(additional_details[0])
                        track_dict.update(additional_info)

                    # Move specific fields to their own variables and keep the rest in details
                    title = track_dict.pop('Title')
                    length = track_dict.pop('Length')
                    number = track_dict.pop('Number')
                    variation_id = track_dict.pop('Variation_id')
                    details = json.dumps(track_dict)

                    cache.append({
                        'title': title,
                        'length': length,
                        'number': number,
                        'variation_id': variation_id,
                        'details': details
                    })

            logger.info(f"Scraped {len(track_elements)} tracks for release ID {release_id}")

        except NoSuchElementException as e:
            logger.error(f"Some elements were not found for release ID {release_id}: {e}")
        finally:
            if driver:
                driver.quit()
                logger.debug(f"Closed ChromeDriver for release ID {release_id}")

def insert_data_into_db(cache, db_connection):
    cursor = db_connection.cursor()

    for track in cache:
        sql = """
        INSERT INTO tracklists (number, title, length, variation_id, detail)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            track['number'],
            track['title'],
            track['length'],
            track['variation_id'],
            track['details']
        ))

    db_connection.commit()
    cursor.close()
    logger.info(f"Inserted {len(cache)} tracks into the database")

def fetch_variation_ids(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT variation_id FROM albumvariations")
    result = cursor.fetchall()
    cursor.close()
    logger.debug(f"Fetched {len(result)} variation IDs from albumvariations")
    return [row[0] for row in result]

def worker(release_id, cache, semaphore):
    logger.debug(f"Worker started for release ID {release_id}")
    db_connection = get_database_connection()
    try:
        scrape_release_data(release_id, cache, semaphore)
        insert_data_into_db(cache, db_connection)
        cache.clear()
    finally:
        db_connection.close()
        logger.debug(f"Worker finished for release ID {release_id}")

# Main script
cache = []

# Semaphore to limit the number of concurrent ChromeDriver instances
semaphore = threading.Semaphore(5)

db_connection = get_database_connection()
release_ids = fetch_variation_ids(db_connection)
db_connection.close()

logger.debug(f"Starting threads for release IDs: {release_ids}")

threads = []
for release_id in release_ids:
    t = threading.Thread(target=worker, args=(release_id, cache, semaphore))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

logger.info("Data scraped and saved correctly.")
