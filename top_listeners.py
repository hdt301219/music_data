import mysql.connector
from datetime import datetime
import logging
from utils import create_database_connection, fetch_top_chart_data
from logging_config import setup_logging

# URL to scrape
URL = "https://kworb.net/spotify/listeners.html"

# Set up logging
setup_logging()


def insert_top_listeners(cursor, table_name, entry):
    try:
        artist = entry.get("Artist")
        peak_listeners = entry.get("PkListeners", "0").replace(',', '')
        listeners = entry.get("Listeners", "0").replace(',', '')
        peak_position = entry.get("Peak", "0").replace(',', '')

        logging.info(f"Inserting: Artist: {artist}, Peak Listeners: {peak_listeners}, Listeners: {listeners}, Peak Position: {peak_position}")

        insert_query = """
        INSERT INTO TopListeners (artist, peak_listeners, listeners, peak_position)
        VALUES (%s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            artist,
            peak_listeners,
            listeners,
            peak_position
        ))
        logging.info("Inserted artist data into database")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting artist data: {err}")

def fetch_and_store_listeners():
    rows = fetch_top_chart_data(URL)
    if not rows:
        logging.error("No data fetched from URL")
        return

    conn, cursor = create_database_connection(
        host="localhost",
        user="spotify_user",
        password="password",
        database="spotify_db"
    )

    if not conn or not cursor:
        logging.error("Database connection failed")
        return

    table_name = "TopListeners"
    conn.commit()

    for entry in rows:
        logging.info(f"Processing entry: {entry}")
        insert_top_listeners(cursor, table_name, entry)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Database connection closed")

if __name__ == "__main__":
    fetch_and_store_listeners()
