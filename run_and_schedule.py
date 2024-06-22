import schedule
import time
import subprocess
import logging
from threading import Timer

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_script(script_name):
    logger.info(f"Starting the script: {script_name}")
    result = subprocess.run(["python", script_name], capture_output=True, text=True)
    logger.info(f"Script {script_name} finished with output:\n{result.stdout}")
    if result.stderr:
        logger.error(f"Script {script_name} encountered errors:\n{result.stderr}")

def run_daily():
    run_script("daily.py")

def run_weekly():
    run_script("weekly.py")

def run_crawl_artist_id():
    run_script("crawlArtistID.py")

def run_crawl_artist_info():
    run_script("crawlArtistInfo.py")

def run_crawl_album_variation_id():
    run_script("crawlAlbumVariationID.py")

def run_crawl_album_variation_info():
    run_script("crawlAlbumVariationInfo.py")

def run_crawl_tracklists():
    run_script("crawlTracklists.py")

def run_top_artists():
    run_script("top_artists.py")

def run_top_listeners():
    run_script("top_listeners.py")

def stop_scheduler():
    logger.info("Stopping the scheduler after running for 1 hour.")
    global scheduler_running
    scheduler_running = False

# Schedule the scripts
schedule.every().day.at("01:00").do(run_daily)
schedule.every().week.do(run_weekly)
schedule.every().day.at("02:00").do(run_crawl_artist_id)
schedule.every().day.at("03:00").do(run_crawl_artist_info)

schedule.every(2).hours.at(":00").do(run_crawl_artist_info)
schedule.every(2).hours.at(":40").do(run_crawl_album_variation_id)
schedule.every(2).hours.at(":40").do(run_crawl_album_variation_info)
schedule.every(2).hours.at(":40").do(run_crawl_tracklists)

schedule.every(14).days.do(run_top_artists)
schedule.every(14).days.do(run_top_listeners)

logger.info("Scheduler started with the defined schedules.")

# Variable to control the scheduler running state
scheduler_running = True

# Timer to stop the scheduler after 1 hour (3600 seconds)
stop_timer = Timer(3600, stop_scheduler)
stop_timer.start()

# Keep the script running for the specified duration
while scheduler_running:
    schedule.run_pending()
    time.sleep(60)  # Check every minute

logger.info("Scheduler has been stopped.")
