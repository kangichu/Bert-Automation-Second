from sentence_transformers import SentenceTransformer
import numpy as np
import logging

# Load SBERT model
MODEL_NAME = "sentence-transformers/all-MiniLM-L6-v2"
logging.info(f"Loading BERT model: {MODEL_NAME}")
model = SentenceTransformer(MODEL_NAME)

def generate_embeddings(narratives):
    """Generate BERT embeddings for multiple property listings."""

    all_embeddings = []  # To store all embeddings

    for narrative in narratives:
        # Compute embeddings for each narrative
        embedding = model.encode(narrative[1], convert_to_numpy=True, normalize_embeddings=True)
        all_embeddings.append(embedding)

    # Convert to numpy array after collecting all embeddings
    return np.array(all_embeddings)