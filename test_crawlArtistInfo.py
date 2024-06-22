import requests
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil
import gc
from db_connection import get_database_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Function to get artist IDs from the database
def get_artist_ids_from_db():
    with get_database_connection() as db:
        with db.cursor() as cursor:
            cursor.execute("SELECT artist_id FROM Artists")
            artist_ids = [row[0] for row in cursor.fetchall()]
    return artist_ids

# Function to insert artist data into the database
def insert_artist_data_to_db(artist_data):
    with get_database_connection() as db:
        with db.cursor() as cursor:
            # Update the Artists table
            update_artist_query = """
            UPDATE Artists
            SET wikipedia_extract = %s, type = %s, founded = %s, dissolved = %s,
                founded_in = %s, area = %s, genres = %s
            WHERE artist_id = %s
            """
            cursor.execute(update_artist_query, (
                artist_data['Wikipedia Extract'], 
                artist_data['Properties'].get('Type'), 
                artist_data['Properties'].get('Founded'), 
                artist_data['Properties'].get('Dissolved'),
                ", ".join(artist_data['Properties'].get('Founded in', [])), 
                ", ".join(artist_data['Properties'].get('Area', [])), 
                ", ".join(artist_data['Genres']), 
                artist_data['Artist ID']
            ))

            # Insert or Update the Album table
            insert_album_query = """
            INSERT INTO Albums (title, year, artist)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE title = VALUES(title), year = VALUES(year), artist = VALUES(artist)
            """
            for album in artist_data['Albums']:
                try:
                    logger.info(f"Inserting/Updating Album: Title={album['Title']}, Year={album['Year']}, Artist={album['Artist']}")
                    cursor.execute(insert_album_query, (
                        album.get('Title'), 
                        album.get('Year'), 
                        album.get('Artist')  # Use artist name from album data
                    ))
                except mysql.connector.Error as err:
                    logger.error(f"Error inserting/updating album: Title={album['Title']}, Year={album['Year']}, Artist={album['Artist']}, Error: {err}")

            # Insert or Update the Single table
            insert_single_query = """
            INSERT INTO Singles (title, year, feature)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE title = VALUES(title), year = VALUES(year), feature = VALUES(feature)
            """
            for single in artist_data['Singles']:
                try:
                    logger.info(f"Inserting/Updating Single: Title={single['Title']}, Year={single['Year']}, Feature={single['Artist']}")
                    cursor.execute(insert_single_query, (
                        single.get('Title'), 
                        single.get('Year'), 
                        single.get('Artist')  # Use artist name from single data
                    ))
                except mysql.connector.Error as err:
                    logger.error(f"Error inserting/updating single: Title={single['Title']}, Year={single['Year']}, Feature={single['Artist']}, Error: {err}")

            db.commit()

# Function to crawl MusicBrainz for artist details
def crawl_musicbrainz(artist_id, retries=3):
    # Set up headless Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")  # Suppress non-critical logs

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 20)

    artist_data = {
        'Artist ID': artist_id,
        'Wikipedia Extract': None,
        'Genres': [],
        'Properties': {},
        'Albums': [],
        'Singles': []
    }

    try:
        driver.get(f"https://musicbrainz.org/artist/{artist_id}")

        # Extracting Wikipedia extract text
        for attempt in range(retries):
            try:
                wikipedia_extract = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.wikipedia-extract-body"))).text
                artist_data['Wikipedia Extract'] = wikipedia_extract
                logger.info(f"Successfully retrieved Wikipedia extract for artist ID {artist_id}")
                break
            except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                logger.warning(f"Retrying Wikipedia extract for artist ID {artist_id}, attempt {attempt + 1}")

        # Extracting genres and other tags
        for attempt in range(retries):
            try:
                genres = [genre.text for genre in wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div#sidebar-tags a")))]
                artist_data['Genres'] = genres
                logger.info(f"Successfully retrieved genres for artist ID {artist_id}")
                break
            except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                logger.warning(f"Retrying genres for artist ID {artist_id}, attempt {attempt + 1}")

        # Extracting properties from the artist information sidebar using the specified XPath
        for attempt in range(retries):
            try:
                properties_elements = wait.until(EC.presence_of_all_elements_located((By.XPATH, "/html/body/div[2]/div[2]/dl/dt")))
                for dt in properties_elements:
                    if "ISNI code" not in dt.text:  # Exclude ISNI code
                        dd = dt.find_element(By.XPATH, "./following-sibling::dd")
                        if dd.find_elements(By.TAG_NAME, 'a'):
                            artist_data['Properties'][dt.text.strip(':')] = [a.text for a in dd.find_elements(By.TAG_NAME, 'a')]
                        else:
                            artist_data['Properties'][dt.text.strip(':')] = dd.text
                            logger.info(f"Retrieved property '{dt.text.strip(':')}' for artist ID {artist_id}: {dd.text}")
                logger.info(f"Successfully retrieved properties for artist ID {artist_id}")
                break
            except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                logger.warning(f"Retrying properties for artist ID {artist_id}, attempt {attempt + 1}")

        # Extracting album details
        for attempt in range(retries):
            try:
                album_section = wait.until(EC.presence_of_element_located((By.XPATH, "//h3[text()='Album']")))
                album_table = album_section.find_element(By.XPATH, "./following-sibling::table")
                album_rows = album_table.find_elements(By.XPATH, ".//tbody/tr")
                for row in album_rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if cells:
                        artist_data['Albums'].append({
                            'Year': cells[0].text,
                            'Title': cells[1].text,
                            'Artist': cells[2].text
                        })
                logger.info(f"Successfully retrieved albums for artist ID {artist_id}")
                break
            except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                logger.warning(f"Retrying albums for artist ID {artist_id}, attempt {attempt + 1}")

        # Extracting single details
        for attempt in range(retries):
            try:
                single_section = wait.until(EC.presence_of_element_located((By.XPATH, "//h3[text()='Single']")))
                single_table = single_section.find_element(By.XPATH, "./following-sibling::table")
                single_rows = single_table.find_elements(By.XPATH, ".//tbody/tr")
                for row in single_rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if cells:
                        artist_data['Singles'].append({
                            'Year': cells[0].text,
                            'Title': cells[1].text,
                            'Artist': cells[2].text
                        })
                logger.info(f"Successfully retrieved singles for artist ID {artist_id}")
                break
            except (NoSuchElementException, TimeoutException, StaleElementReferenceException):
                logger.warning(f"Retrying singles for artist ID {artist_id}, attempt {attempt + 1}")

    finally:
        driver.quit()
        del driver
    
    insert_artist_data_to_db(artist_data)

    # Free memory
    del artist_data
    gc.collect()

# Fetch artist IDs from the database
artist_ids = get_artist_ids_from_db()

# Adjust number of workers based on system capacity
def get_optimal_worker_count():
    cpu_count = psutil.cpu_count(logical=False)  # Physical cores
    total_memory = psutil.virtual_memory().total / (1024 ** 3)  # Convert bytes to GB

    # Assuming each worker/thread requires some amount of RAM, e.g., 1GB per worker
    worker_count = min(cpu_count * 2, total_memory // 1)

    # Further adjust if necessary, based on system testing
    return max(1, int(worker_count))

# Crawl data and insert into database for each artist ID using concurrency
def main():
    optimal_worker_count = get_optimal_worker_count()
    logger.info(f"Using {optimal_worker_count} workers for crawling")

    with ThreadPoolExecutor(max_workers=optimal_worker_count) as executor:
        futures = {executor.submit(crawl_musicbrainz, artist_id): artist_id for artist_id in artist_ids}
        for future in as_completed(futures):
            artist_id = futures[future]
            try:
                future.result()
                logger.info(f"Data for artist ID {artist_id} has been inserted into the database.")
            except Exception as e:
                logger.error(f"Error processing artist ID {artist_id}: {e}")

if __name__ == "__main__":
    main()
