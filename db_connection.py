import mysql.connector

def get_database_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="spotify_user",
        password="password",
        database="spotify_db"
    )
    return connection
