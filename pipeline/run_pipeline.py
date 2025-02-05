import logging
import faiss
import numpy as np
from utils.logger import setup_logging
from handlers.mysql_data_fetch.fetch import fetch_data_from_mysql
from handlers.data_handling.data_handling  import format_data
from handlers.embeddings_generation.generate_embeddings  import generate_embeddings
from handlers.embeddings_storage.embeddings_storage  import train_faiss_index, store_embeddings_in_trained_index
from handlers.listings_tracker.tracker import ListingsTracker

def run_pipeline(train_only=False, storage=False, index_file="faiss_index_ivfpq.bin"):
    logging.info(f"Arguments passed to function: train_only={train_only}, storage={storage}")

    # Initialize listings tracker
    tracker = ListingsTracker()

    # Step 1: Fetch data from MySQL
    logging.info('Fetching Data form MySQL Database')
    listings  = fetch_data_from_mysql()

    if not listings:
        logging.error("No data fetched from MySQL.")
        return

    # Step 2: Format data retrived from MySQL Database
    logging.info('Formatting Data into narratives')
    narratives, listing_ids = format_data(listings)

    if narratives is None:
        logging.error("Failed to generate formatted data.")
        return

    # Step 3: Convert narratives data to BERT embeddings
    logging.info('Convert narratives data to BERT embeddings')
    embeddings = generate_embeddings(narratives)

    if embeddings is None:
        logging.error("Failed to generate embeddings.")
        return

    logging.info(f"Generated {len(embeddings)} embeddings.")
    logging.info(f"Embeddings shape: {embeddings.shape}")

    # Step 4: Training phase if `train_only` is True
    if train_only:
        logging.info('Training FAISS index...')
        index = train_faiss_index(embeddings)
        logging.info('FAISS index training complete.')
        return  # Exit after training if in training mode only
 
    # Step 5: Storing Embeddings into FAISS (Local Storage)
    if storage:
        try:
            # Try loading the existing index
            try:
                index = faiss.read_index(index_file)
                logging.info("Loaded existing FAISS index.")
            except Exception as e:
                logging.warning(f"Could not load FAISS index: {str(e)}. Training a new one.")
                index = train_faiss_index(embeddings)

            # Ensure the index is trained before adding embeddings
            if not index.is_trained:
                logging.warning("Loaded FAISS index is not trained. Training now...")
                index.train(embeddings)
                faiss.write_index(index, index_file)
                logging.info("FAISS index training completed and saved.")

            # Store embeddings in the trained index
            store_embeddings_in_trained_index(embeddings, index, listing_ids, index_file)

            # Track stored listings
            tracker.initialize_mappings(listing_ids)
            logging.info(f"Tracked {len(listing_ids)} listings in initial index")

            logging.info("Embeddings stored in FAISS.")

        except Exception as e:
            logging.error(f"Error processing FAISS index: {str(e)}")
            return


    # Step 6: Verify Storage (after storage phase)
    logging.info('Verifying Embeddings Storage in FAISS')
    try:
        loaded_index = faiss.read_index("faiss_index_ivfpq.bin")
        if loaded_index.ntotal != embeddings.shape[0]:
            raise ValueError("Stored vector count does not match input.")
        _, I = loaded_index.search(np.array([embeddings[0]]), k=1)  # Check if search works
        logging.info("Embeddings storage verified.")
    except Exception as e:
        logging.error(f"Failed to verify embeddings storage: {str(e)}")
        return  # Handle accordingly

    # Step 5: Verify Storage
    logging.info('Verifying Embeddings Storage in FAISS')
    try:
        loaded_index = faiss.read_index("faiss_index_ivfpq.bin")
        if loaded_index.ntotal != embeddings.shape[0]:
            raise ValueError("Stored vector count does not match input.")
        _, I = loaded_index.search(np.array([embeddings[0]]), k=1)  # Check if search works
        logging.info("Embeddings storage verified.")
    except Exception as e:
        logging.error(f"Failed to verify embeddings storage: {str(e)}")
        return  # Handle accordingly