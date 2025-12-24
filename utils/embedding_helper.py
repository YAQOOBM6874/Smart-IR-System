"""
Embedding Helper
Provides a singleton-like interface to the sentence-transformers model
for generating text embeddings. Uses free, local models.
"""

from typing import List, Union
import numpy as np
from sentence_transformers import SentenceTransformer

class EmbeddingHelper:
    """
    Helper class to manage the embedding model and encode text using free models
    """
    _instance = None
    _model = None

    def __new__(cls, model_name: str = 'all-MiniLM-L6-v2'):
        if cls._instance is None:
            cls._instance = super(EmbeddingHelper, cls).__new__(cls)
            try:
                # This model is free, open-source, and runs locally
                print(f"Loading free local embedding model: {model_name}...")
                cls._model = SentenceTransformer(model_name)
                print("Model loaded successfully.")
            except Exception as e:
                print(f"Error loading embedding model: {e}")
                cls._model = None
        return cls._instance

    def encode(self, text: Union[str, List[str]]) -> List[float]:
        """
        Convert text or list of texts into embeddings
        
        Args:
            text: Single string or list of strings
            
        Returns:
            A list of floats (embedding) or a list of lists of floats
        """
        if self._model is None:
            raise RuntimeError("Embedding model not loaded.")
        
        # Ensure model is on CPU/GPU as appropriate
        embeddings = self._model.encode(text)
        
        # Convert numpy array to list for JSON serialization in Elasticsearch
        if isinstance(text, str):
            return embeddings.tolist()
        else:
            return [emb.tolist() for emb in embeddings]

    def get_dimension(self) -> int:
        """
        Get the dimension of the embeddings produced by the model
        """
        if self._model is None:
            return 384  # Default for all-MiniLM-L6-v2
        return self._model.get_sentence_embedding_dimension()
