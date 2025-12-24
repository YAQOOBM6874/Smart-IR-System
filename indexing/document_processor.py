"""
Document Processor
Handles document ingestion, processing, and indexing into Elasticsearch
"""

import os
import json
from datetime import datetime
from typing import List, Dict, Optional
from elasticsearch import Elasticsearch, helpers
from bs4 import BeautifulSoup
import re

from config.elasticsearch_config import get_elasticsearch_config, INDEX_SETTINGS
from indexing.temporal_extractor import TemporalExtractor
from indexing.geo_extractor import GeoExtractor
from utils.embedding_helper import EmbeddingHelper


class DocumentProcessor:
    """
    Processes and indexes documents into Elasticsearch
    """
    
    def __init__(self, es_host: str = "http://localhost:9200"):
        """
        Initialize the document processor
        
        Args:
            es_host: Elasticsearch host URL
        """
        self.es = Elasticsearch([es_host])
        self.config = get_elasticsearch_config()
        self.index_name = self.config["index_name"]
        
        # Initialize extractors
        self.temporal_extractor = TemporalExtractor()
        self.geo_extractor = GeoExtractor()
        self.embedding_helper = EmbeddingHelper()
        
        # Create index if it doesn't exist
        self._create_index()
    
    def _create_index(self):
        """
        Create the Elasticsearch index with proper mappings and settings
        """
        if not self.es.indices.exists(index=self.index_name):
            self.es.indices.create(index=self.index_name, body=INDEX_SETTINGS)
            print(f"Created index: {self.index_name}")
        else:
            print(f"Index already exists: {self.index_name}")
    
    def delete_index(self):
        """
        Delete the index (useful for testing)
        """
        if self.es.indices.exists(index=self.index_name):
            self.es.indices.delete(index=self.index_name)
            print(f"Deleted index: {self.index_name}")
    
    def _clean_html(self, text: str) -> str:
        """
        Remove HTML tags from text
        
        Args:
            text: Text potentially containing HTML
            
        Returns:
            Cleaned text
        """
        if not text:
            return ""
        
        soup = BeautifulSoup(text, "lxml")
        return soup.get_text()
    
    def _parse_authors(self, authors_data: any) -> List[Dict[str, str]]:
        """
        Parse authors into the required nested structure
        
        Args:
            authors_data: Can be a string, list of strings, or list of dicts
            
        Returns:
            List of author dictionaries with first_name, last_name, email
        """
        if not authors_data:
            return []
        
        authors = []
        
        # Handle different input formats
        if isinstance(authors_data, str):
            # Single author as string
            authors_data = [authors_data]
        
        for author in authors_data:
            if isinstance(author, dict):
                # Already in correct format
                authors.append({
                    "first_name": author.get("first_name", ""),
                    "last_name": author.get("last_name", ""),
                    "email": author.get("email", "")
                })
            elif isinstance(author, str):
                # Parse string "FirstName LastName <email@example.com>"
                email_match = re.search(r'<(.+?)>', author)
                email = email_match.group(1) if email_match else ""
                
                name_part = re.sub(r'<.+?>', '', author).strip()
                name_parts = name_part.split()
                
                first_name = name_parts[0] if len(name_parts) > 0 else ""
                last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
                
                authors.append({
                    "first_name": first_name,
                    "last_name": last_name,
                    "email": email
                })
        
        return authors
    
    def process_document(self, doc_data: Dict[str, any]) -> Dict[str, any]:
        """
        Process a single document and extract all required fields
        
        Args:
            doc_data: Dictionary containing document data with keys:
                - title: Document title
                - content: Document content
                - authors: List of authors (optional)
                - date: Publication date (optional)
                - geopoint: Geopoint coordinates (optional)
                - metadata: Structured metadata from source (optional)
                
        Returns:
            Processed document ready for indexing
        """
        # Extract basic fields
        title = doc_data.get("title", "")
        content = self._clean_html(doc_data.get("content", ""))
        
        # Combine title and content for extraction
        full_text = f"{title} {content}"
        
        # Check if we have structured metadata (e.g., from Reuters)
        metadata = doc_data.get("metadata", {})
        
        # Extract temporal expressions
        # Prioritize structured data if available
        if metadata.get("reuters_date"):
            # Use Reuters date directly
            temporal_expressions = [metadata["reuters_date"]]
            parsed_dates = []
            try:
                from dateparser import parse
                parsed = parse(metadata["reuters_date"])
                if parsed:
                    parsed_dates = [parsed]
            except:
                pass
        else:
            # Fall back to text extraction
            temporal_result = self.temporal_extractor.extract_temporal_expressions(full_text)
            temporal_expressions = temporal_result["temporal_expressions"]
            parsed_dates = temporal_result["parsed_dates"]
        
        # Extract georeferences
        # Prioritize structured data if available
        if metadata.get("reuters_places"):
            # Use Reuters places directly
            georeferences = metadata["reuters_places"]
            geocoded_locations = []
            # Try to geocode the places
            for place in georeferences[:5]:  # Limit to first 5 to avoid slowdown
                geocoded = self.geo_extractor.geocode_location(place)
                if geocoded:
                    geocoded_locations.append(geocoded)
        else:
            # Fall back to text extraction
            geo_result = self.geo_extractor.extract_and_geocode(full_text)
            georeferences = geo_result["georeferences"]
            geocoded_locations = geo_result["geocoded_locations"]
        
        # Parse authors
        authors = self._parse_authors(doc_data.get("authors", []))
        
        # Determine date (use provided date or approximate from temporal expressions)
        doc_date = doc_data.get("date")
        if not doc_date and parsed_dates:
            doc_date = max(parsed_dates)
        elif not doc_date:
            doc_date = datetime.now()
        
        # Convert to ISO format if it's a datetime object
        if isinstance(doc_date, datetime):
            doc_date = doc_date.strftime("%Y-%m-%dT%H:%M:%S")
        
        # Determine geopoint (use provided geopoint or approximate from georeferences)
        geopoint = doc_data.get("geopoint")
        if not geopoint and geocoded_locations:
            geopoint = {
                "lat": geocoded_locations[0]["lat"],
                "lon": geocoded_locations[0]["lon"]
            }
        
        # Build the document
        processed_doc = {
            "title": title,
            "content": content,
            "authors": authors,
            "date": doc_date,
            "temporal_expressions": temporal_expressions,
            "georeferences": georeferences,
        }
        
        # Add optional fields
        if geopoint:
            processed_doc["geopoint"] = geopoint
        
        if parsed_dates:
            processed_doc["extracted_dates"] = [d.isoformat() for d in parsed_dates]
        
        if geocoded_locations:
            processed_doc["extracted_locations"] = [
                {
                    "name": loc["name"],
                    "coordinates": {
                        "lat": loc["lat"],
                        "lon": loc["lon"]
                    }
                }
                for loc in geocoded_locations
            ]
        
        # Generate semantic embedding for the content
        if content:
            processed_doc["content_vector"] = self.embedding_helper.encode(content)
        
        return processed_doc
    
    def index_document(self, doc_data: Dict[str, any], doc_id: Optional[str] = None) -> Dict[str, any]:
        """
        Process and index a single document
        
        Args:
            doc_data: Document data dictionary
            doc_id: Optional document ID
            
        Returns:
            Elasticsearch response
        """
        processed_doc = self.process_document(doc_data)
        
        if doc_id:
            response = self.es.index(index=self.index_name, id=doc_id, document=processed_doc)
        else:
            response = self.es.index(index=self.index_name, document=processed_doc)
        
        return response
    
    def index_documents_bulk(self, documents: List[Dict[str, any]]) -> Dict[str, any]:
        """
        Index multiple documents in bulk
        
        Args:
            documents: List of document data dictionaries
            
        Returns:
            Bulk indexing statistics
        """
        actions = []
        
        for doc_data in documents:
            processed_doc = self.process_document(doc_data)
            
            action = {
                "_index": self.index_name,
                "_source": processed_doc
            }
            
            # Add document ID if provided
            if "id" in doc_data:
                action["_id"] = doc_data["id"]
            
            actions.append(action)
        
        # Perform bulk indexing
        success, failed = helpers.bulk(self.es, actions, stats_only=True)
        
        return {
            "success": success,
            "failed": failed,
            "total": len(documents)
        }
    
    def index_from_json_file(self, file_path: str) -> Dict[str, any]:
        """
        Index documents from a JSON file
        
        Args:
            file_path: Path to JSON file containing documents
            
        Returns:
            Indexing statistics
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            documents = json.load(f)
        
        if isinstance(documents, dict):
            documents = [documents]
        
        return self.index_documents_bulk(documents)
    
    def index_from_directory(self, directory_path: str) -> Dict[str, any]:
        """
        Index all JSON files from a directory
        
        Args:
            directory_path: Path to directory containing JSON files
            
        Returns:
            Indexing statistics
        """
        total_success = 0
        total_failed = 0
        
        for filename in os.listdir(directory_path):
            if filename.endswith('.json'):
                file_path = os.path.join(directory_path, filename)
                print(f"Indexing {filename}...")
                
                result = self.index_from_json_file(file_path)
                total_success += result["success"]
                total_failed += result["failed"]
        
        return {
            "success": total_success,
            "failed": total_failed,
            "total": total_success + total_failed
        }


if __name__ == "__main__":
    # Test the document processor
    processor = DocumentProcessor()
    
    # Sample document
    sample_doc = {
        "title": "Climate Change Conference 2023",
        "content": "The annual climate change conference was held in Paris on March 15, 2023. Researchers from London and New York presented their findings.",
        "authors": [
            "John Doe <john@example.com>",
            "Jane Smith <jane@example.com>"
        ]
    }
    
    # Index the document
    response = processor.index_document(sample_doc)
    print(f"Indexed document: {response}")
