import logging
import faiss
import numpy as np
from utils.logger import setup_logging
from handlers.mysql_data_fetch.fetch import fetch_new_listings
from handlers.data_handling.data_handling import format_data
from handlers.embeddings_generation.generate_embeddings import generate_embeddings
from handlers.embeddings_storage.embeddings_storage import store_embeddings_in_trained_index, check_and_retrain_index
from handlers.listings_tracker.tracker import ListingsTracker

def update_pipeline(index_file="faiss_index_ivfpq.bin"):
    logging.info("Starting update pipeline...")

    # Initialize listings tracker
    tracker = ListingsTracker()

    # Step 1: Fetch new data from MySQL
    logging.info('Fetching new data from MySQL Database')
    new_listings = fetch_new_listings()

    if not new_listings:
        logging.error("No new data fetched from MySQL.")
        return

    # Step 2: Format new data
    logging.info('Formatting new data into narratives')
    new_narratives, new_listing_ids = format_data(new_listings)

    if new_narratives is None:
        logging.error("Failed to generate formatted data for new listings.")
        return

    # Step 3: Convert new narratives data to BERT embeddings
    logging.info('Convert new narratives data to BERT embeddings')
    new_embeddings = generate_embeddings(new_narratives)

    if new_embeddings is None:
        logging.error("Failed to generate embeddings for new listings.")
        return

    logging.info(f"Generated {len(new_embeddings)} new embeddings.")
    logging.info(f"New embeddings shape: {new_embeddings.shape}")

    # Step 4: Load existing FAISS index and update with new embeddings
    try:
        index = faiss.read_index(index_file)
        logging.info("Loaded existing FAISS index.")
    except Exception as e:
        logging.error(f"Could not load FAISS index: {str(e)}")
        return

    # Step 5: Store new embeddings in the FAISS index
    try:
        start_idx = index.ntotal
        store_embeddings_in_trained_index(new_embeddings, index, index_file)

        # Update tracker with new listings
        tracker.update_mappings(new_listing_ids, start_idx)
        logging.info(f"Added {len(new_listing_ids)} listings to tracker")

        logging.info("New embeddings stored in FAISS index.")
    except Exception as e:
        logging.error(f"Error storing new embeddings in FAISS index: {str(e)}")
        return

    # Step 6: Verify Storage
    logging.info('Verifying new embeddings storage in FAISS')
    try:
        loaded_index = faiss.read_index(index_file)
        if loaded_index.ntotal != index.ntotal + new_embeddings.shape[0]:
            raise ValueError("Stored vector count does not match expected count.")
        _, I = loaded_index.search(np.array([new_embeddings[0]]), k=1)  # Check if search works
        logging.info("New embeddings storage verified.")
    except Exception as e:
        logging.error(f"Failed to verify new embeddings storage: {str(e)}")
        return