import json
import datetime
import logging
from pathlib import Path

class ListingsTracker:
    def __init__(self, mapping_file="listings_mapping.json"):
        self.mapping_file = Path(mapping_file)
        self.id_to_position = {}  # listing_id -> faiss_position
        self.position_to_id = {}  # faiss_position -> listing_id
        self.total_embeddings = 0
        self.load_mappings()
    
    def load_mappings(self):
        """Load existing ID-position mappings"""
        try:
            if self.mapping_file.exists():
                with open(self.mapping_file, 'r') as f:
                    data = json.load(f)
                    mappings = data.get('listings', {})
                    # Convert string keys to integers
                    self.id_to_position = {int(k): int(v) for k, v in mappings.items()}
                    self.position_to_id = {v: k for k, v in self.id_to_position.items()}
                    self.total_embeddings = data.get('total_embeddings', len(self.id_to_position))
                logging.info(f"Loaded {len(self.id_to_position)} existing mappings")
        except Exception as e:
            logging.error(f"Error loading mappings: {e}")
    
    def initialize_mappings(self, listing_ids):
        """Store initial listings after first FAISS creation"""
        for idx, lid in enumerate(listing_ids):
            self.id_to_position[lid] = idx
            self.position_to_id[idx] = lid
        self.total_embeddings = len(listing_ids)
        self.save_mappings()
    
    def add_mappings(self, new_mappings):
        """Add new listing ID to FAISS position mappings"""
        try:
            for listing_id, position in new_mappings.items():
                self.id_to_position[listing_id] = position
                self.position_to_id[position] = listing_id
            self.total_embeddings += len(new_mappings)
            self.save_mappings()
            logging.info(f"Added {len(new_mappings)} new mappings")
        except Exception as e:
            logging.error(f"Error adding mappings: {e}")
            raise
    
    def save_mappings(self):
        """Save current mappings to file"""
        try:
            with open(self.mapping_file, 'w') as f:
                json.dump({
                    'listings': self.id_to_position,
                    'total_embeddings': self.total_embeddings,
                    'last_updated': datetime.datetime.now().isoformat()
                }, f)
            logging.info(f"Saved {len(self.id_to_position)} mappings")
        except Exception as e:
            logging.error(f"Error saving mappings: {e}")
            raise
    
    def get_listing_id(self, faiss_position):
        """Get listing ID from FAISS position"""
        return self.position_to_id.get(faiss_position)
    
    def get_faiss_position(self, listing_id):
        """Get FAISS position from listing ID"""
        return self.id_to_position.get(listing_id)