import logging
import faiss
import numpy as np
from pathlib import Path
from handlers.listings_tracker.tracker import ListingsTracker

RETRAIN_THRESHOLD = 0.5  # If new embeddings are ≥ 50% of stored ones, retrain
# Create storage directory
INDEX_DIR = Path("storage/faiss_indices")
INDEX_DIR.mkdir(parents=True, exist_ok=True)

def get_all_existing_embeddings(index_file):
    """
    Retrieve all stored embeddings from the FAISS index.

    :param index_file: Path to FAISS index file
    :return: numpy array of stored embeddings
    """
    index = faiss.read_index(index_file)
    stored_embeddings = np.zeros((index.ntotal, index.d), dtype='float32')
    
    # Retrieve stored embeddings
    for i in range(index.ntotal):
        stored_embeddings[i] = index.reconstruct(i)

    return stored_embeddings

def train_faiss_index(embeddings, nlist=100, m=8, index_file="faiss_index_ivfpq.bin"):
    """
    Train a FAISS index using IVF+PQ method.
    """
    try:
        embeddings = embeddings.astype('float32')
        dimension = embeddings.shape[1]
        num_points = embeddings.shape[0]
        
        # Validate data size
        if num_points < 4000:
            logging.warning(f"Small dataset ({num_points} points). Adjusting parameters.")
            nlist = min(nlist, max(int(num_points/40), 4))
            m = min(m, max(int(dimension/32), 4))
        
        logging.info(f"Training with {num_points} points, {nlist} clusters, {m} subquantizers")
        
        # Set up index path
        index_path = INDEX_DIR / index_file
        
        # Create quantizer
        quantizer = faiss.IndexFlatL2(dimension)
        
        # Create and train index
        index = faiss.IndexIVFPQ(quantizer, dimension, nlist, m, 8)
        
        if not index.is_trained:
            logging.info("Training FAISS index...")
            index.train(embeddings)
        
        faiss.write_index(index, str(index_path))
        logging.info(f"Index saved to {index_path}")
        return index
        
    except Exception as e:
        logging.error(f"Error training FAISS index: {e}")
        raise

def store_embeddings_in_trained_index(embeddings, index, index_file="faiss_index_ivfpq.bin"):
    """
    Store embeddings in a trained FAISS index using IVF+PQ method.

    :param embeddings: numpy array of embeddings
    :param index: The trained FAISS index
    :param index_file: Name of the file to store the FAISS index
    :return: None
    """
    embeddings = embeddings.astype('float32')
    
    if not index.is_trained:
        logging.error("Attempted to add embeddings to an untrained index!")
        return

    # Add embeddings to the trained index
    logging.info(f"Adding {embeddings.shape[0]} embeddings to the FAISS index...")
    index.add(embeddings)

    # Save the index to disk
    faiss.write_index(index, index_file)
    logging.info(f"Embeddings have been stored in FAISS IVFPQ index: {index_file}")


def check_and_retrain_index(embeddings, index, index_file="faiss_index_ivfpq.bin", retrain_threshold=0.5):
    """
    Check if retraining is needed based on the amount of new embeddings and retrain if necessary.

    :param embeddings: numpy array of new embeddings
    :param index: The trained FAISS index
    :param index_file: Path to save the updated index
    :param retrain_threshold: Fraction of existing embeddings above which retraining occurs
    :return: Updated FAISS index
    """
    existing_embeddings_count = index.ntotal  
    new_embeddings_count = embeddings.shape[0]

    logging.info(f"Existing embeddings: {existing_embeddings_count}, New embeddings: {new_embeddings_count}")

    if existing_embeddings_count > 0 and (new_embeddings_count / existing_embeddings_count) >= retrain_threshold:
        logging.warning("Significant new data detected. Retraining FAISS index...")
        
        all_embeddings = np.vstack((get_all_existing_embeddings(index_file), embeddings))
        index = train_faiss_index(all_embeddings, index_file=index_file)
        logging.info("Retraining completed.")

    return index  # Return the potentially retrained index


def store_embeddings_in_trained_index(embeddings, index, listing_ids, index_file="faiss_index_ivfpq.bin"):
    """
    Store embeddings in a trained FAISS index.

    :param embeddings: numpy array of new embeddings
    :param index: The trained FAISS index
    :param index_file: Path to save the updated index
    :return: None
    """
    try:
        tracker = ListingsTracker()
        embeddings = embeddings.astype('float32')

        if not index.is_trained:
            logging.error("Attempted to add embeddings to an untrained index!")
            return

        # Get current position before adding
        current_position = index.ntotal

        # First, check if retraining is needed
        index = check_and_retrain_index(embeddings, index, index_file)

        logging.info(f"Adding {embeddings.shape[0]} embeddings to FAISS index...")
        index.add(embeddings)

        # Create position mappings after successful addition
        position_map = {
            listing_id: current_position + idx 
            for idx, listing_id in enumerate(listing_ids)
        }
        
        # Update tracker with new mappings
        tracker.add_mappings(position_map)

        faiss.write_index(index, index_file)
        logging.info(f"Updated FAISS index stored at {index_file}")
        logging.info(f"Added {len(listing_ids)} listings to position mapping")
        return True
    
    except Exception as e:
        logging.error(f"Error storing embeddings: {e}")
        return False


