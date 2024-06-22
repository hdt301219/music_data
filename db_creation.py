import mysql.connector

# Database connection
db = mysql.connector.connect(
    host="localhost",
    user="spotify_user",
    password="password",
    database="spotify_db"
)
cursor = db.cursor()

# Create tables
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Artists (
        artist_id VARCHAR(255) PRIMARY KEY,
        artist VARCHAR(255) NOT NULL,
        wikipedia_extract VARCHAR(255),
        type VARCHAR(50),
        founded VARCHAR(255),
        dissolved VARCHAR(255),
        founded_in VARCHAR(255),
        area VARCHAR(255),
        genres VARCHAR(255)
    );
""")

# Create Albums table with index on album_id
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Albums (
        no INT AUTO_INCREMENT PRIMARY KEY,
        album_id VARCHAR(255),
        artist VARCHAR(255) NOT NULL,
        description VARCHAR(255),
        artist_id VARCHAR(255),
        genres VARCHAR(255),
        other_tags VARCHAR(255),
        image VARCHAR(255),
        title VARCHAR(255) NOT NULL,
        year INT,
        FOREIGN KEY (artist_id) REFERENCES Artists(artist_id),
        INDEX (album_id)
    );
""")

# Create Singles table
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Singles (
        no INT AUTO_INCREMENT PRIMARY KEY,
        single_id VARCHAR(255),
        title VARCHAR(255) NOT NULL,
        year YEAR NOT NULL,
        feature VARCHAR(255) NOT NULL
    );
""")

# Create AlbumVariations table with correct foreign key constraints
cursor.execute("""
    CREATE TABLE IF NOT EXISTS AlbumVariations (
        no INT AUTO_INCREMENT PRIMARY KEY,
        variation_id VARCHAR(255),
        album_id VARCHAR(255),
        title VARCHAR(255),
        variation_title VARCHAR(255),
        format VARCHAR(255),
        artist_id VARCHAR(255),
        artist VARCHAR(255),
        year YEAR,
        tracks INT,
        country_date VARCHAR(255),
        labels VARCHAR(255),
        FOREIGN KEY (artist_id) REFERENCES Artists(artist_id),
        FOREIGN KEY (album_id) REFERENCES Albums(album_id),
        INDEX (album_id),
        INDEX (artist_id),
        INDEX (variation_id)
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS TopListeners (
        chart_id INT AUTO_INCREMENT PRIMARY KEY,
        artist_id VARCHAR(255),
        artist VARCHAR(255),
        peak_listeners VARCHAR(255),
        listeners VARCHAR(255),
        peak_position VARCHAR(255),
        FOREIGN KEY (artist_id) REFERENCES Artists(artist_id)
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS TopArtists (
        chart_id INT AUTO_INCREMENT PRIMARY KEY,
        artist_id VARCHAR(255),
        artist VARCHAR(255), 
        streams VARCHAR(255),
        daily VARCHAR(255),
        as_lead VARCHAR(255),
        solo VARCHAR(255),
        as_feature VARCHAR(255),
        FOREIGN KEY (artist_id) REFERENCES Artists(artist_id)
    );
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS TrackLists (
        artist_id VARCHAR(255),
        album_id VARCHAR(255),
        number VARCHAR(255),
        title VARCHAR(255),
        variation_id VARCHAR(255),
        length VARCHAR(255),
        detail VARCHAR(255),
        FOREIGN KEY (artist_id) REFERENCES Artists(artist_id),
        FOREIGN KEY (album_id) REFERENCES Albums(album_id),
        FOREIGN KEY (variation_id) REFERENCES AlbumVariations(variation_id),
        INDEX (variation_id)
    );
""")

# Close the cursor and connection
cursor.close()
db.close()
