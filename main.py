import os
import sys
import logging
import time
import signal
import argparse
import asyncio
from utils.logger import setup_logging
from dataset.dataset_generation import generate_dataset

def load_run_pipeline():
    from pipeline.run_pipeline import run_pipeline
    return run_pipeline

def load_update_pipeline():
    from pipeline.update_pipeline import update_pipeline
    return update_pipeline

def load_db_watcher():
    from utils.watcher import DBWatcher
    return DBWatcher

def main():
    setup_logging()

    # Set the environment variable to disable oneDNN custom operations
    os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

    # Verify the environment variable was set
    if 'TF_ENABLE_ONEDNN_OPTS' in os.environ and os.environ['TF_ENABLE_ONEDNN_OPTS'] == '0':
        logging.info("Environment variable TF_ENABLE_ONEDNN_OPTS has been set to '0'.")
    else:
        logging.error("Failed to set environment variable TF_ENABLE_ONEDNN_OPTS to '0'.")
        return  # or handle this as needed

    try:
        # If no argument is passed, show the available options
        print("\nHere are the items that can be run:")
        options = {
            "1": "generate_dataset - generate synthetic listing data for training the model. This is the first step in the pipeline",
            "2": "run_pipeline --train-only - only train the model, converts listings to embeddings without storage. This is the second step in the pipeline",
            "3": "run_pipeline --storage-only - only store embeddings, assumes embeddings are already generated. This is the third step in the pipeline",
            "4": "update_pipeline - this is when you're adding new listings into the FAISS database. This is the fourth step in the pipeline"
        }
        for key, value in options.items():
            print(f"  {key}. {value}")

        choice = input("\nPlease select the number representing the function you want to run: ").strip()
        
        if choice == '1':
            # Only this option runs async
            num_listings = int(input("Number of listings (default 2000): ") or 2000)
            batch_size = int(input("Batch size (default 50): ") or 50)
            asyncio.run(generate_dataset(num_listings, batch_size))
        
        elif choice in ['2', '3']:
            # from pipeline.run_pipeline import run_pipeline
            run_pipeline = load_run_pipeline()
            train_only = choice == '2'
            storage_only = choice == '3'
            success = run_pipeline(train_only=train_only, storage=storage_only)

            if train_only and success:
                logging.info("Training completed successfully")
                return

            if not success:
                logging.error("Pipeline failed to complete successfully")
                return

            if storage_only:
                try:
                    # Start watcher thread
                    DBWatcher = load_db_watcher()
                    watcher = DBWatcher()
                    watcher.start()
                    logging.info("DB Watcher started successfully")
                    
                    try:
                        while True:
                            time.sleep(1)
                    except KeyboardInterrupt:
                        logging.info("Shutting down watcher...")
                        watcher.stop()
                        watcher.join()
                        logging.info("Watcher shutdown complete")
                except Exception as e:
                    logging.error(f"Error in watcher thread: {e}")
                    if 'watcher' in locals():
                        watcher.stop()
                        watcher.join()

        elif choice == '4':
            # from pipeline.update_pipeline import update_pipeline
            update_pipeline = load_update_pipeline()
            update_pipeline()

        else:
            print("\nInvalid choice. Please run the script again with a valid option.")
            sys.exit(1)
    
    except KeyboardInterrupt:
        logging.info("\nProcess interrupted by user (Ctrl+C). Cleaning up and exiting...")
        sys.exit(0)  # Exit gracefully


if __name__ == "__main__":
    main()