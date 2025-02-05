import json
import datetime
import logging
from pathlib import Path

class ListingsTracker:
    def __init__(self, tracking_file="listing_mapping.json"):
        self.tracking_file = Path(tracking_file)
        self.listing_map = self._load_mapping()
        self.total_embeddings = len(self.listing_map)

    def initialize_mappings(self, listing_ids):
        """Store initial listings after first FAISS creation"""
        for idx, lid in enumerate(listing_ids):
            self.listing_map[str(lid)] = idx
        self.total_embeddings = len(listing_ids)
        self._save_mapping()
        
    def _save_mapping(self):
        with open(self.tracking_file, 'w') as f:
            json.dump({
                'listings': self.listing_map,
                'total_embeddings': self.total_embeddings,
                'last_updated': datetime.now().isoformat()
            }, f)

    def update_mappings(self, listing_ids, start_idx):
        """Update mappings with new listings and their FAISS indices"""
        for idx, lid in enumerate(listing_ids):
            self.listing_map[str(lid)] = start_idx + idx
        self.total_embeddings += len(listing_ids)
        self._save_mapping()