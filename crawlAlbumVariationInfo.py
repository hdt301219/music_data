import json
import logging
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from concurrent.futures import ThreadPoolExecutor
from db_connection import get_database_connection
from threading import Semaphore

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

semaphore = Semaphore(5)

def scrape_musicbrainz_data(release_group_id):
    with semaphore:
        logger.info(f"Starting data scrape for release group ID: {release_group_id}")

        # Set up headless Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--log-level=3")  # Suppress non-critical logs

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        url = f"https://musicbrainz.org/release-group/{release_group_id}"
        driver.get(url)

        data = {}
        try:
            # Extract Wikipedia description
            try:
                wikipedia_description = driver.find_element(By.CSS_SELECTOR, "div.wikipedia-extract-body").text
                data['Wikipedia Description'] = wikipedia_description
                logger.info(f"Extracted Wikipedia description for release group ID: {release_group_id}")
            except NoSuchElementException:
                data['Wikipedia Description'] = None
                logger.warning(f"Wikipedia description not found for release group ID: {release_group_id}")

            # Extract release information
            releases = []
            try:
                release_rows = driver.find_elements(By.XPATH, "//table[contains(@class, 'tbl')]/tbody/tr")
                for row in release_rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if cells:
                        try:
                            variation_id = cells[0].find_element(By.XPATH, ".//a[2]").get_attribute('href').split('/')[-1]
                        except NoSuchElementException:
                            variation_id = None
                        releases.append({
                            'Variation ID': variation_id,
                            'Title': cells[0].text,
                            'Artist': cells[1].text,
                            'Format': cells[2].text,
                            'Tracks': cells[3].text,
                            'Country/Date': cells[4].text,
                            'Label': cells[5].text
                        })
                data['Releases'] = releases
            except NoSuchElementException:
                data['Releases'] = None
                logger.warning(f"Release information not found for release group ID: {release_group_id}")
            
            # Extract the album cover image, artist, album type, genres, and other tags
            try:
                data['Album Image URL'] = driver.find_element(By.CSS_SELECTOR, "div.cover-art img").get_attribute('src')
                data['Artist'] = driver.find_element(By.XPATH, "//dt[text()='Artist:']/following-sibling::dd").text
                data['Album Type'] = driver.find_element(By.XPATH, "//dt[text()='Type:']/following-sibling::dd").text
                data['Genres'] = [element.text for element in driver.find_elements(By.CSS_SELECTOR, "div#sidebar-tags div.genre-list a")]
                data['Other Tags'] = [element.text for element in driver.find_elements(By.CSS_SELECTOR, "div#sidebar-tag-list a")]
                logger.info(f"Extracted additional information for release group ID: {release_group_id}")
            except NoSuchElementException:
                data['Album Image URL'] = None
                data['Artist'] = None
                data['Album Type'] = None
                data['Genres'] = None
                data['Other Tags'] = None
                logger.warning(f"Additional information not found for release group ID: {release_group_id}")

        finally:
            driver.quit()
            del driver

        logger.info(f"Completed data scrape for release group ID: {release_group_id}")
        return data


def update_database(data, release_group_id):
    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        # Update albums table
        sql_update_albums = """
        UPDATE albums
        SET description=%s, image=%s, genres=%s, other_tags=%s
        WHERE album_id=%s
        """
        cursor.execute(sql_update_albums, (
            data.get('Wikipedia Description'),
            data.get('Album Image URL'),
            json.dumps(data.get('Genres')),
            json.dumps(data.get('Other Tags')),
            release_group_id
        ))

        # Insert into albumvariations table
        if data.get('Releases'):
            for release in data['Releases']:
                sql_insert_variations = """
                INSERT INTO albumvariations (album_id, title, artist, format, tracks, country_date, labels, Variation_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql_insert_variations, (
                    release_group_id,
                    release['Title'],
                    release['Artist'],
                    release['Format'],
                    release['Tracks'],
                    release['Country/Date'],
                    release['Label'],
                    release['Variation ID']
                ))

        connection.commit()
        logger.info(f"Database updated for release group ID: {release_group_id}")
    except Exception as e:
        logger.error(f"Error updating database for release group ID: {release_group_id}: {e}")
    finally:
        cursor.close()
        connection.close()

def worker(release_group_id):
    data = scrape_musicbrainz_data(release_group_id)
    update_database(data, release_group_id)

def main():
    connection = get_database_connection()
    cursor = connection.cursor()
    album_ids = []

    try:
        cursor.execute("SELECT album_id FROM albums")
        album_ids = [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()
        connection.close()

    with ThreadPoolExecutor(max_workers=5) as executor:
        for album_id in album_ids:
            executor.submit(worker, album_id)

if __name__ == "__main__":
    main()
