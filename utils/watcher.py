import time
import threading
import logging
from datetime import datetime
from handlers.mysql_data_fetch.fetch import fetch_new_listings
from pipeline.update_pipeline import update_pipeline
from handlers.listings_tracker.tracker import ListingsTracker

class DBWatcher(threading.Thread):
    def __init__(self, check_interval=300):  # 5 minutes default
        super().__init__()
        self.stop_flag = threading.Event()
        self.check_interval = check_interval
        self.tracker = ListingsTracker()
        
    def check_for_new_listings(self):
        """Check database for new listings not in tracker"""
        try:
            return fetch_new_listings(self.tracker)
        except Exception as e:
            logging.error(f"Error checking for new listings: {e}")
            return None
        
    def run(self):
        logging.info("Starting DB watcher thread...")
        while not self.stop_flag.is_set():
            try:
                new_listings = self.check_for_new_listings()
                if new_listings:
                    logging.info(f"Found {len(new_listings)} new listings")
                    update_pipeline()
            except Exception as e:
                logging.error(f"Error in watcher: {e}")
            time.sleep(self.check_interval)
    
    def stop(self):
        logging.info("Stopping DB watcher...")
        self.stop_flag.set()