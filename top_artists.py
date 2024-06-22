import mysql.connector
from datetime import datetime
import logging
from utils import create_database_connection, fetch_top_chart_data
from logging_config import setup_logging

# URL to scrape
URL = "https://kworb.net/spotify/artists.html"

# Set up logging
setup_logging()

def insert_top_artists(cursor, table_name, entry):
    try:
        artist = entry.get("Artist")
        streams = entry.get("Streams", "0")
        daily = entry.get("Daily", "0")
        as_lead = entry.get("As lead", "0")
        solo = entry.get("Solo", "0")
        as_feature = entry.get("As feature", "0")
        
        # Log the values before insertion
        logging.info(f"Artist: {artist} data is received")

        insert_query = """
        INSERT INTO TopArtists (artist, streams, daily, as_lead, solo, as_feature)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        cursor.execute(insert_query, (
            artist,
            streams,
            daily,
            as_lead,
            solo,
            as_feature
        ))
        logging.info("Inserted artist data into database")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting artist data: {err}")
    except ValueError as ve:
        logging.error(f"Value error: {ve}")
    except KeyError as ke:
        logging.error(f"Key error: {ke}")

def fetch_and_store_artists():
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

    table_name = "TopArtists"
    conn.commit()

    for entry in rows:
        logging.info(f"Processing entry: {entry}")
        insert_top_artists(cursor, table_name, entry)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Database connection closed")

if __name__ == "__main__":
    fetch_and_store_artists()
