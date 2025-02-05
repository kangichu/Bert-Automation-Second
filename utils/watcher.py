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
        self.check_interval = check_interval
        self.stop_flag = threading.Event()
        self.tracker = ListingsTracker()
        
    def run(self):
        logging.info("Starting DB watcher...")
        while not self.stop_flag.is_set():
            if fetch_new_listings(self.tracker):
                logging.info("New listings detected, running update pipeline...")
                update_pipeline()
            time.sleep(self.check_interval)
            
    def stop(self):
        self.stop_flag.set()