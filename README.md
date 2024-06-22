
## Project Overview

This project aims to scrape data from MusicBrainz and Kworb.net to track various aspects of music artists, albums, and tracks, along with their performance on weekly and top charts. The relationships between the tables are primarily established using foreign keys, ensuring data integrity and enabling complex queries to derive insights from the dataset. The data crawled from these two networks cover many aspects of daily, weekly, and top-tier lists of artists and songs. This project aims to investigate deeply into modern music and contribute to the development of a recommendation system.

Installation Instructions:
1. Make sure you have MySQL installed
2. Setup MySQL following this .env file:
    export SPOTIFY_CLIENT_ID='54b11003414242439787b52d628f8dee'
    export SPOTIFY_CLIENT_SECRET='a16f62cebe9c406aa4eef3a9fb7f3deb'
    export DB_HOST='localhost'
    export DB_USER='spotify_user'
    export DB_PASSWORD='password'
    export DB_NAME='spotify_db'
3. Install requirements file which located in this file.
4. Run db_creation.py file to create database.
5. Run run_and_schedule.py file to run crawling system.

Running Description
The scheduling script (scheduler.py) performs the following tasks:

1.Logging Setup: Initializes logging to track the execution of scripts, including any output or errors.
2.Script Execution Function: Defines a function run_script that takes a script name as an argument and executes it using subprocess.run. The function logs the start and end of the script execution, along with any output or errors.
3.Individual Script Functions: Defines individual functions for running each script. These functions call run_script with the appropriate script name.
4.Scheduling the Scripts: Uses the schedule library to define when each script should run. The schedules include:
    Daily scripts
    Weekly scripts
    Scripts that run every 2 hours
    Scripts that run every 14 days
5.Stopping the Scheduler: Includes a function to stop the scheduler after one hour. This is managed by a timer.
6.Main Scheduler Loop: Runs the scheduler loop, checking every minute if any scheduled tasks need to be executed.

## Database Schema Explanation

### Table: `artists`
- **artist_id**: `VARCHAR(255)` - Unique identifier for the artist.
- **artist**: `VARCHAR(255)` - Name of the artist.
- **wikipedia_extract**: `TEXT` - Extract from Wikipedia about the artist.
- **type**: `VARCHAR(50)` - Type or category of the artist.
- **founded**: `VARCHAR(255)` - Founding date of the artist or band.
- **founded_in**: `VARCHAR(255)` - Location where the artist or band was founded.
- **area**: `VARCHAR(255)` - Geographical area related to the artist.
- **genres**: `VARCHAR(255)` - Genres associated with the artist.

### Table: `singles`
- **no**: `INT` - Sequence number.
- **single_id**: `VARCHAR(255)` - Unique identifier for the single.
- **title**: `VARCHAR(255)` - Title of the single.
- **year**: `YEAR` - Release year of the single.
- **feature**: `VARCHAR(255)` - Featured artists in the single.

### Table: `spotifyweekly_20240622`
- **chart_id**: `INT` - Unique identifier for the chart.
- **position**: `INT` - Position of the track in the chart.
- **position_change**: `VARCHAR(10)` - Change in position from the previous week.
- **title**: `VARCHAR(255)` - Title of the track.
- **week_on_chart**: `INT` - Number of weeks the track has been on the chart.
- **peak_position**: `INT` - Highest position achieved by the track.
- **x_count**: `VARCHAR(10)` - Number of times the track was played.
- **streams_change**: `BIGINT` - Change in the number of streams.
- **total_streams**: `BIGINT` - Total number of streams.
- **week_ending**: `DATE` - Week ending date for the chart.

### Table: `albums`
- **no**: `INT` - Sequence number.
- **album_id**: `VARCHAR(255)` - Unique identifier for the album.
- **artist_id**: `VARCHAR(255)` - Foreign key referencing `artists`.
- **artist**: `VARCHAR(255)` - Name of the artist.
- **genres**: `VARCHAR(255)` - Genres associated with the album.
- **other_tags**: `VARCHAR(255)` - Additional tags for the album.
- **image**: `VARCHAR(255)` - Image associated with the album.
- **title**: `VARCHAR(255)` - Title of the album.
- **year**: `INT` - Release year of the album.

### Table: `spotifytopchart_20240622`
- **chart_id**: `INT` - Unique identifier for the chart.
- **position**: `INT` - Position of the track in the chart.
- **position_change**: `VARCHAR(10)` - Change in position from the previous week.
- **artist**: `VARCHAR(255)` - Name of the artist.
- **title**: `VARCHAR(255)` - Title of the track.
- **days_on_chart**: `INT` - Number of days the track has been on the chart.
- **peak_position**: `INT` - Highest position achieved by the track.
- **streams**: `BIGINT` - Number of streams.
- **streams_change**: `BIGINT` - Change in the number of streams.
- **seven_day_streams**: `BIGINT` - Number of streams in the last seven days.
- **total_streams**: `BIGINT` - Total number of streams.
- **date**: `DATE` - Date of the chart.

### Table: `tracklists`
- **artist_id**: `VARCHAR(255)` - Foreign key referencing `artists`.
- **album_id**: `VARCHAR(255)` - Foreign key referencing `albums`.
- **number**: `VARCHAR(255)` - Track number in the album.
- **title**: `VARCHAR(255)` - Title of the track.
- **variation_id**: `VARCHAR(255)` - Variation identifier for the track.
- **length**: `VARCHAR(255)` - Length of the track.
- **detail**: `VARCHAR(255)` - Additional details about the track.

### Table: `albumvariations`
- **no**: `INT` - Sequence number.
- **album_id**: `VARCHAR(255)` - Foreign key referencing `albums`.
- **title**: `VARCHAR(255)` - Title of the album.
- **variation_id**: `VARCHAR(255)` - Variation identifier for the album.
- **listeners**: `VARCHAR(255)` - Number of listeners.
- **year**: `YEAR` - Release year of the album.
- **tracks**: `INT` - Number of tracks in the album.
- **country_code**: `VARCHAR(255)` - Country code associated with the album.
- **labels**: `VARCHAR(255)` - Labels associated with the album.

### Table: `toplisteners`
- **chart_id**: `INT` - Unique identifier for the chart.
- **artist_id**: `VARCHAR(255)` - Foreign key referencing `artists`.
- **artist**: `VARCHAR(255)` - Name of the artist.
- **peak_listeners**: `VARCHAR(255)` - Peak number of listeners.
- **listeners**: `VARCHAR(255)` - Number of listeners.
- **peak_position**: `VARCHAR(255)` - Peak position of the artist in the chart.

### Table: `topartists`
- **chart_id**: `INT` - Unique identifier for the chart.
- **artist_id**: `VARCHAR(255)` - Foreign key referencing `artists`.
- **artist**: `VARCHAR(255)` - Name of the artist.
- **streams**: `VARCHAR(255)` - Number of streams.
- **daily**: `VARCHAR(255)` - Daily streams.
- **as_lead**: `VARCHAR(255)` - Streams as lead artist.
- **solo**: `VARCHAR(255)` - Streams as solo artist.
- **as_feature**: `VARCHAR(255)` - Streams as featured artist.

### Table: `meta`
- **id**: `INT` - Unique identifier.
- **latest_table_name**: `VARCHAR(255)` - Name of the latest table.
