import mysql.connector
from datetime import datetime
import logging
from utils import create_database_connection, fetch_top_chart_data
from logging_config import setup_logging

# URL to scrape
URL = "https://kworb.net/spotify/country/global_weekly.html"

# Set up logging
setup_logging()

def create_chart_table(cursor, week_ending):
    table_name = f"SpotifyWeekly_{week_ending.strftime('%Y%m%d')}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        chart_id INT AUTO_INCREMENT PRIMARY KEY,
        position INT,
        position_change VARCHAR(10),
        artist VARCHAR(255),
        title VARCHAR(255),
        weeks_on_chart INT,
        peak_position INT,
        x_count VARCHAR(10),
        streams BIGINT,
        streams_change BIGINT,
        total_streams BIGINT,
        week_ending DATE
    );
    """
    try:
        cursor.execute(create_table_query)
        logging.info(f"Table {table_name} created or already exists")
    except mysql.connector.Error as err:
        logging.error(f"Error creating table {table_name}: {err}")
    return table_name

def insert_top_chart_data(cursor, table_name, entry, week_ending):
    insert_query = f"""
    INSERT INTO {table_name} (position, position_change, artist, title, weeks_on_chart, peak_position, x_count, streams, streams_change, total_streams, week_ending)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    def safe_int(value):
        return int(value.replace(',', '').replace('+', '').replace('-', '')) if value.replace(',', '').replace('+', '').replace('-', '') else 0

    try:
        cursor.execute(insert_query, (
            int(entry["Pos"]),
            entry["P+"],
            entry["Artist and Title"].split(" - ")[0].strip(),
            entry["Artist and Title"].split(" - ")[1].strip(),
            int(entry["Wks"]),
            int(entry["Pk"]),
            entry["(x?)"],
            int(entry["Streams"].replace(',', '')),
            safe_int(entry["Streams+"]),
            int(entry["Total"].replace(',', '')),
            week_ending
        ))
        logging.info(f"Inserted data for artist {entry['Artist and Title'].split(' - ')[0].strip()} and track {entry['Artist and Title'].split(' - ')[1].strip()}")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting top chart data: {err}")

def fetch_and_store_weekly():
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

    week_ending = datetime.now().date()
    table_name = create_chart_table(cursor, week_ending)
    conn.commit()

    try:
        for entry in rows:
            if not insert_top_chart_data(cursor, table_name, entry, week_ending):
                continue
        conn.commit()
    except Exception as e:
        conn.rollback()
        logging.error(f"An error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    fetch_and_store_weekly()
