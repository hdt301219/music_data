import requests
import mysql.connector
from concurrent.futures import ThreadPoolExecutor, as_completed
import psutil

def find_release_group_id(artist_name, album_name):
    base_url = "https://musicbrainz.org/ws/2/release-group"
    params = {
        "fmt": "json",
        "query": f'artist:"{artist_name}" AND releasegroup:"{album_name}"'
    }
    response = requests.get(base_url, params=params)
    response_json = response.json()
    
    if response_json['release-groups']:
        return response_json['release-groups'][0]['id']
    else:
        return None

def get_database_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="spotify_user",
        password="password",
        database="spotify_db"
    )
    return connection

def fetch_and_update_album(album):
    no, artist_name, album_name = album
    release_group_id = find_release_group_id(artist_name, album_name)
    return (release_group_id, no) if release_group_id else None

def monitor_resources():
    cpu_usage = psutil.cpu_percent(interval=1)
    memory_info = psutil.virtual_memory()
    print(f"CPU Usage: {cpu_usage}% | Available Memory: {memory_info.available / (1024 ** 2)} MB")

def update_albums_with_release_group_id():
    conn = get_database_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT no, artist, title FROM albums")
    albums = cursor.fetchall()

    # Start with a lower number of workers and adjust based on performance
    num_workers = 12  # Starting with the number of physical cores

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(fetch_and_update_album, album): album for album in albums}
        updates = []
        for future in as_completed(futures):
            result = future.result()
            if result:
                updates.append(result)

            # Monitor resources periodically
            monitor_resources()

    if updates:
        cursor.executemany("""
            UPDATE albums
            SET album_id = %s
            WHERE no = %s
        """, updates)
        conn.commit()

    cursor.close()
    conn.close()

# Run the update function
update_albums_with_release_group_id()
