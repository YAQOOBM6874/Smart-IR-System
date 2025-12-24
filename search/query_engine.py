"""
Query Engine
Handles all search and retrieval operations including autocomplete, 
spatiotemporal queries, and analytics
"""

from typing import List, Dict, Optional, Tuple
from datetime import datetime
from elasticsearch import Elasticsearch
from config.elasticsearch_config import get_elasticsearch_config
from utils.embedding_helper import EmbeddingHelper


class QueryEngine:
    """
    Handles all query operations for the Smart Document Retrieval System
    """
    
    def __init__(self, es_host: str = "http://localhost:9200"):
        """
        Initialize the query engine
        
        Args:
            es_host: Elasticsearch host URL
        """
        self.es = Elasticsearch([es_host])
        self.config = get_elasticsearch_config()
        self.index_name = self.config["index_name"]
        self.embedding_helper = EmbeddingHelper()
    
    def autocomplete(self, query: str, size: int = 10) -> List[Dict[str, any]]:
        """
        Autocomplete search on document titles
        Starts suggesting after 3 characters with fuzzy matching
        
        Args:
            query: Search query (should be at least 3 characters)
            size: Number of results to return (default: 10)
            
        Returns:
            List of matching documents
        """
        if len(query) < 3:
            return []
        
        search_body = {
            "size": size,
            "query": {
                "bool": {
                    "should": [
                        {
                            "match": {
                                "title": {
                                    "query": query,
                                    "fuzziness": "AUTO",
                                    "boost": 2
                                }
                            }
                        },
                        {
                            "match_phrase_prefix": {
                                "title": {
                                    "query": query,
                                    "boost": 3
                                }
                            }
                        }
                    ]
                }
            },
            "_source": ["title", "date", "authors"]
        }
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        results = []
        for hit in response["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                "title": hit["_source"]["title"],
                "date": hit["_source"].get("date"),
                "authors": hit["_source"].get("authors", []),
                "score": hit["_score"]
            })
        
        return results
    
    def search(
        self,
        query: str,
        temporal_expression: Optional[str] = None,
        georeference: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        location: Optional[Tuple[float, float]] = None,
        distance: str = "100km",
        size: int = 10,
        semantic_weight: float = 0.5
    ) -> List[Dict[str, any]]:
        """
        Perform spatiotemporal search with lexical and semantic retrieval
        
        Args:
            query: Text query
            temporal_expression: Temporal expression filter
            georeference: Georeference filter
            date_from: Start date for temporal range
            date_to: End date for temporal range
            location: (lat, lon) tuple for geo search
            distance: Distance for geo search (e.g., "100km")
            size: Number of results to return
            
        Returns:
            List of matching documents with scores
        """
        # Build the query
        must_clauses = []
        should_clauses = []
        filter_clauses = []
        
        # Text search with emphasis on title
        if query:
            should_clauses.extend([
                {
                    "multi_match": {
                        "query": query,
                        "fields": ["title^10", "content"],
                        "type": "best_fields",
                        "fuzziness": "AUTO"
                    }
                },
                {
                    "multi_match": {
                        "query": query,
                        "fields": ["title.standard^5", "content"],
                        "type": "phrase",
                        "boost": 2
                    }
                }
            ])
        
        # Temporal expression filter (as a filter, not should)
        if temporal_expression:
            # Try to parse the temporal expression
            import dateparser
            parsed_date = dateparser.parse(temporal_expression)
            
            if parsed_date:
                # Better date range handling:
                # If the string contains "1987" and "February" but no specific day, 
                # we should cover the whole month.
                date_str = temporal_expression.lower()
                is_only_month_year = any(m in date_str for m in ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december', 'jan', 'feb', 'mar', 'apr', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']) and not any(char.isdigit() for char in date_str.replace('1987', '').replace('1988', ''))
                
                if is_only_month_year or len(temporal_expression) < 10:
                    # Assume it's a month or year
                    date_start = parsed_date.replace(day=1, hour=0, minute=0, second=0)
                    from datetime import timedelta
                    # Next month start - 1 second
                    if parsed_date.month == 12:
                        date_end = parsed_date.replace(year=parsed_date.year + 1, month=1, day=1, hour=0, minute=0, second=0) - timedelta(seconds=1)
                    else:
                        date_end = parsed_date.replace(month=parsed_date.month + 1, day=1, hour=0, minute=0, second=0) - timedelta(seconds=1)
                else:
                    date_start = parsed_date.replace(hour=0, minute=0, second=0)
                    date_end = parsed_date.replace(hour=23, minute=59, second=59)
                
                filter_clauses.append({
                    "range": {
                        "date": {
                            "gte": date_start.isoformat(),
                            "lte": date_end.isoformat()
                        }
                    }
                })
            else:
                # Fallback to searching in temporal_expressions text field or extracted_dates
                filter_clauses.append({
                    "multi_match": {
                        "query": temporal_expression,
                        "fields": ["temporal_expressions", "title", "content"]
                    }
                })
        
        # Georeference filter (as a filter, not should)
        if georeference:
            filter_clauses.append({
                "match": {
                    "georeferences": georeference
                }
            })
        
        # Date range filter
        if date_from or date_to:
            date_range = {}
            if date_from:
                date_range["gte"] = date_from
            if date_to:
                date_range["lte"] = date_to
            
            filter_clauses.append({
                "range": {
                    "date": date_range
                }
            })
        
        # Geo distance filter
        if location:
            filter_clauses.append({
                "geo_distance": {
                    "distance": distance,
                    "geopoint": {
                        "lat": location[0],
                        "lon": location[1]
                    }
                }
            })
        
        # Build the complete query
        query_body = {
            "bool": {}
        }
        
        if should_clauses:
            query_body["bool"]["should"] = should_clauses
            query_body["bool"]["minimum_should_match"] = 1
        else:
            # If no text query, use match_all to ensure proper scoring
            query_body["bool"]["must"] = [{"match_all": {}}]
        
        if filter_clauses:
            query_body["bool"]["filter"] = filter_clauses
        
        # Apply weighting to the lexical part
        query_body["bool"]["boost"] = 1.0 - semantic_weight
        
        # Generate query vector for semantic search
        query_vector = self.embedding_helper.encode(query) if query else None
        
        # Add function score for recency and localization
        search_body = {
            "size": size,
            "query": {
                "function_score": {
                    "query": query_body,
                    "functions": [
                        {
                            "gauss": {
                                "date": {
                                    "origin": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                                    "scale": "365d",
                                    "decay": 0.5
                                }
                            },
                            "weight": 2
                        }
                    ],
                    "score_mode": "sum",
                    "boost_mode": "sum"
                }
            }
        }
        
        # Add kNN for semantic search if vector is available and weight > 0
        if query_vector and semantic_weight > 0:
            search_body["knn"] = {
                "field": "content_vector",
                "query_vector": query_vector,
                "k": size,
                "num_candidates": 100,
                "boost": semantic_weight * 10 # Signficantly boost kNN to ensure it has impact
            }
            
            # If we have filters, we MUST apply them to kNN as well
            if filter_clauses:
                search_body["knn"]["filter"] = filter_clauses
        
        # Add geo distance sorting if location is provided
        if location:
            search_body["sort"] = [
                "_score",
                {
                    "_geo_distance": {
                        "geopoint": {
                            "lat": location[0],
                            "lon": location[1]
                        },
                        "order": "asc",
                        "unit": "km"
                    }
                }
            ]
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        max_score = response["hits"].get("max_score") or 0
        results = []
        for hit in response["hits"]["hits"]:
            # Normalize score to be 1 or less
            normalized_score = hit["_score"] / max_score if max_score > 0 else 0
            
            result = {
                "id": hit["_id"],
                "score": normalized_score,
                **hit["_source"]
            }
            
            # Add distance if geo search was performed
            if location and "sort" in hit:
                result["distance_km"] = hit["sort"][1]
            
            results.append(result)
        
        return results
    
    def get_top_georeferences(self, size: int = 10) -> List[Dict[str, any]]:
        """
        Get the top mentioned georeferences across the entire index
        
        Args:
            size: Number of top georeferences to return
            
        Returns:
            List of georeferences with counts
        """
        search_body = {
            "size": 0,
            "aggs": {
                "top_georeferences": {
                    "terms": {
                        "field": "georeferences",
                        "size": size
                    }
                }
            }
        }
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        results = []
        for bucket in response["aggregations"]["top_georeferences"]["buckets"]:
            results.append({
                "georeference": bucket["key"],
                "count": bucket["doc_count"]
            })
        
        return results
    
    def get_document_distribution_over_time(
        self,
        interval: str = "1d",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> List[Dict[str, any]]:
        """
        Get the distribution of documents over time
        
        Args:
            interval: Time interval for aggregation (default: "1d" for 1 day)
            date_from: Start date for the range
            date_to: End date for the range
            
        Returns:
            List of time buckets with document counts
        """
        search_body = {
            "size": 0,
            "aggs": {
                "documents_over_time": {
                    "date_histogram": {
                        "field": "date",
                        "calendar_interval": interval,
                        "format": "yyyy-MM-dd",
                        "min_doc_count": 0
                    }
                }
            }
        }
        
        # Add date range filter if provided
        if date_from or date_to:
            date_range = {}
            if date_from:
                date_range["gte"] = date_from
            if date_to:
                date_range["lte"] = date_to
            
            search_body["query"] = {
                "range": {
                    "date": date_range
                }
            }
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        results = []
        for bucket in response["aggregations"]["documents_over_time"]["buckets"]:
            results.append({
                "date": bucket["key_as_string"],
                "count": bucket["doc_count"]
            })
        
        return results
    
    def get_document_by_id(self, doc_id: str) -> Optional[Dict[str, any]]:
        """
        Retrieve a document by its ID
        
        Args:
            doc_id: Document ID
            
        Returns:
            Document data or None if not found
        """
        try:
            response = self.es.get(index=self.index_name, id=doc_id)
            return {
                "id": response["_id"],
                **response["_source"]
            }
        except:
            return None
    
    def get_index_stats(self) -> Dict[str, any]:
        """
        Get statistics about the index
        
        Returns:
            Dictionary with index statistics
        """
        count_response = self.es.count(index=self.index_name)
        
        return {
            "total_documents": count_response["count"],
            "index_name": self.index_name
        }


if __name__ == "__main__":
    # Test the query engine
    engine = QueryEngine()
    
    # Test autocomplete
    print("Autocomplete for 'cli':")
    results = engine.autocomplete("cli")
    for result in results:
        print(f"  - {result['title']}")
    
    # Test search
    print("\nSearch for 'climate change':")
    results = engine.search("climate change")
    for result in results:
        print(f"  - {result['title']} (score: {result['score']})")
    
    # Test top georeferences
    print("\nTop georeferences:")
    results = engine.get_top_georeferences()
    for result in results:
        print(f"  - {result['georeference']}: {result['count']}")
    
    # Test document distribution
    print("\nDocument distribution over time:")
    results = engine.get_document_distribution_over_time()
    for result in results[:5]:  # Show first 5
        print(f"  - {result['date']}: {result['count']}")
