import mysql.connector
from datetime import datetime
import logging
from utils import create_database_connection, fetch_top_chart_data


# URL to scrape
URL = "https://kworb.net/spotify/country/global_daily.html"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def create_meta_table(cursor):
    create_meta_table_query = """
    CREATE TABLE IF NOT EXISTS Meta (
        id INT AUTO_INCREMENT PRIMARY KEY,
        latest_table_name VARCHAR(255)
    );
    """
    try:
        cursor.execute(create_meta_table_query)
        logging.info("Meta table created or already exists")
    except mysql.connector.Error as err:
        logging.error(f"Error creating meta table: {err}")

def update_meta_table(cursor, table_name):
    try:
        cursor.execute("DELETE FROM Meta")
        cursor.execute("INSERT INTO Meta (latest_table_name) VALUES (%s)", (table_name,))
        logging.info(f"Meta table updated with latest table name: {table_name}")
    except mysql.connector.Error as err:
        logging.error(f"Error updating meta table: {err}")

def create_chart_table_daily(cursor, date):
    table_name = f"SpotifyTopChart_{date}"
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        chart_id INT AUTO_INCREMENT PRIMARY KEY,
        position INT,
        position_change VARCHAR(10),
        artist VARCHAR(255),
        title VARCHAR(255),
        days_on_chart INT,
        peak_position INT,
        streams BIGINT,
        streams_change BIGINT,
        seven_day_streams BIGINT,
        seven_day_change BIGINT,
        total_streams BIGINT,
        date DATE
    );
    """
    try:
        cursor.execute(create_table_query)
        logging.info(f"Table {table_name} created or already exists")
    except mysql.connector.Error as err:
        logging.error(f"Error creating table {table_name}: {err}")
    return table_name

def insert_top_chart_data(cursor, table_name, entry):
    insert_query = f"""
    INSERT INTO {table_name} (position, position_change, artist, title, days_on_chart, peak_position, streams, streams_change, seven_day_streams, seven_day_change, total_streams, date)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Handle empty strings by replacing them with a default value, e.g., 0
    def safe_int(value):
        return int(value.replace(',', '').replace('+', '').replace('-', '')) if value.replace(',', '').replace('+', '').replace('-', '') else 0

    try:
        cursor.execute(insert_query, (
            int(entry["Pos"]),
            entry["P+"],
            entry["Artist and Title"].split(" - ")[0].strip(),
            entry["Artist and Title"].split(" - ")[1].strip(),
            int(entry["Days"]),
            int(entry["Pk"]),
            int(entry["Streams"].replace(',', '')),
            safe_int(entry["Streams+"]),
            int(entry["7Day"].replace(',', '')),
            safe_int(entry["7Day+"]),
            int(entry["Total"].replace(',', '')),
            datetime.now().date()
        ))
        logging.info(f"Inserted data for artist {entry['Artist and Title'].split(' - ')[0].strip()} and track {entry['Artist and Title'].split(' - ')[1].strip()}")
    except mysql.connector.Error as err:
        logging.error(f"Error inserting top chart data for artist {entry['Artist and Title'].split(' - ')[0].strip()} and track {entry['Artist and Title'].split(' - ')[1].strip()}: {err}")
    except ValueError as ve:
        logging.error(f"Value error for artist {entry['Artist and Title'].split(' - ')[0].strip()} and track {entry['Artist and Title'].split(' - ')[1].strip()}: {ve}")

def fetch_and_store_daily():
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

    # Create meta table if not exists
    create_meta_table(cursor)

    date_str = datetime.now().strftime('%Y%m%d')
    table_name = create_chart_table_daily(cursor, date_str)
    conn.commit()

    # Update meta table with the latest table name
    update_meta_table(cursor, table_name)

    for entry in rows:
        insert_top_chart_data(cursor, table_name, entry)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Database connection closed")

# Execute the function to test
fetch_and_store_daily()
