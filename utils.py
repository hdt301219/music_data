import mysql.connector
import logging
from bs4 import BeautifulSoup
import requests

def fetch_top_chart_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info("Data fetched successfully")
    except requests.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    table = soup.find("table", {"class": "sortable"})
    
    if not table:
        logging.error("No table found on the page")
        return None
    
    headers = [th.text.strip() for th in table.find_all("th")]
    rows = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        row = {headers[i]: cell.text.strip() for i, cell in enumerate(cells)}
        rows.append(row)

    logging.info("Data parsed successfully")
    return rows

def create_database_connection(host, user, password, database):
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='spotify_user',
            password='password',
            database='spotify_db'
        )
        cursor = conn.cursor()
        logging.info("Database connection established")
        return conn, cursor
    except mysql.connector.Error as err:
        logging.error(f"Error connecting to database: {err}")
        return None, None

def insert_artist(cursor, artist_name):
    try:
        cursor.execute("INSERT INTO Artists (name) VALUES (%s) ON DUPLICATE KEY UPDATE name=name", (artist_name,))
        cursor.execute("SELECT artist_id FROM Artists WHERE name=%s", (artist_name,))
        return cursor.fetchone()[0]
    except mysql.connector.Error as err:
        logging.error(f"Error inserting artist {artist_name}: {err}")
        return None

def insert_track(cursor, track_title, artist_id):
    try:
        cursor.execute("INSERT INTO Tracks (title, artist_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE title=title", (track_title, artist_id))
        cursor.execute("SELECT track_id FROM Tracks WHERE title=%s AND artist_id=%s", (track_title, artist_id))
        return cursor.fetchone()[0]
    except mysql.connector.Error as err:
        logging.error(f"Error inserting track {track_title}: {err}")
        return None
