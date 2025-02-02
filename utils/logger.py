import logging
import logging.handlers
import os
import zipfile
from datetime import datetime

# Ensure the logs directory exists
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

def zip_old_logs(log_file):
    try:
        # Check if the log file exists before zipping
        if os.path.exists(log_file):
            zip_file = f"{log_file}.zip"
            
            # Create a zip file and add the log file to it
            with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(log_file, os.path.basename(log_file))
            
            # After zipping, remove the old log file
            os.remove(log_file)
            print(f"Zipped and removed: {log_file}")
    except Exception as e:
        print(f"Error zipping log file {log_file}: {e}")

def setup_logging():
    if not logging.getLogger().hasHandlers():
        # Log file path, based on current date
        log_filename = os.path.join(log_dir, datetime.now().strftime('%Y-%m-%d') + ".log")
        
        # Create a TimedRotatingFileHandler to log daily and keep 7 previous logs
        handler = logging.handlers.TimedRotatingFileHandler(
            log_filename,
            when='midnight',  # Log file rotates at midnight
            interval=1,  # Every day
            backupCount=7,  # Keep the last 7 days' worth of logs
            encoding='utf-8'
        )
        
        # Set up the logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        # Zip previous day's log if it's the first log of the new day
        log_file_to_zip = os.path.join(log_dir, f"{(datetime.now().day - 1) % 31}.log")
        zip_old_logs(log_file_to_zip)

        # Optionally, log to the console as well (you can remove this if not needed)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Test logging
        logger.info("Logging system is set up.")
        return logger
    else:
        print("Logging handlers already set up.")

        
# Initialize logging
logger = setup_logging()
