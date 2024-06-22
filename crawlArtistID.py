import requests
import time
from db_connection import get_database_connection

# Simple in-memory cache to store already fetched artist details
artist_cache = {}

def get_top_artist_id_and_name(artist_name, retries=3, backoff_factor=2):
    if artist_name in artist_cache:
        return artist_cache[artist_name]

    base_url = "https://musicbrainz.org/ws/2/artist/"  # Use HTTPS
    params = {
        "query": f'artist:"{artist_name}"',
        "fmt": "json",
        "limit": 1
    }
    headers = {"User-Agent": "YourAppName/1.0 (YourContactInfo)"}

    for attempt in range(retries):
        try:
            response = requests.get(base_url, headers=headers, params=params, timeout=20)  # Increase timeout
            response.raise_for_status()  # This will raise an exception for HTTP errors
            data = response.json()
            if 'artists' in data and data['artists']:
                top_artist = data['artists'][0]
                artist_cache[artist_name] = (top_artist['name'], top_artist['id'])
                return top_artist['name'], top_artist['id']
            else:
                return None, None
        except requests.RequestException as e:
            print(f"HTTP Request failed (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(backoff_factor ** attempt)  # Exponential backoff
            else:
                return None, None
        except ValueError:
            print("Error decoding JSON")
            return None, None

def get_latest_table_name(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT latest_table_name FROM meta") 
    result = cursor.fetchone()
    return result[0] if result else None

def fetch_artists_from_db(db_connection, table_name):
    cursor = db_connection.cursor()
    cursor.execute(f"SELECT DISTINCT artist FROM {table_name}")
    return [item[0] for item in cursor.fetchall()]

def store_artist_data(db_connection, artist_name, artist_id):
    cursor = db_connection.cursor()
    # Insert new artist or update existing artist's name
    cursor.execute("INSERT INTO Artists (artist, artist_id) VALUES (%s, %s) ON DUPLICATE KEY UPDATE artist_id = VALUES(artist_id)",
                   (artist_name, artist_id))
    db_connection.commit()

def main():
    db_connection = get_database_connection()
    latest_table_name = get_latest_table_name(db_connection)
    if latest_table_name:
        artist_names = fetch_artists_from_db(db_connection, latest_table_name)
        for artist_name in artist_names:
            name, artist_id = get_top_artist_id_and_name(artist_name)
            if name and artist_id:
                store_artist_data(db_connection, name, artist_id)
            # Add a delay between requests to avoid hitting rate limits
            time.sleep(1)  # Adjust the delay as needed (e.g., 1 second)
    else:
        print("Latest table name could not be found in the metadata table.")

    db_connection.close()

if __name__ == "__main__":
    main()
