"""
Analytics Module
Provides analytics and aggregation capabilities for the document index
"""

from typing import List, Dict, Optional
from elasticsearch import Elasticsearch
from config.elasticsearch_config import get_elasticsearch_config


class Analytics:
    """
    Provides analytics and aggregations for the document index
    """
    
    def __init__(self, es_host: str = "http://localhost:9200"):
        """
        Initialize the analytics module
        
        Args:
            es_host: Elasticsearch host URL
        """
        self.es = Elasticsearch([es_host])
        self.config = get_elasticsearch_config()
        self.index_name = self.config["index_name"]
    
    def get_temporal_distribution(
        self,
        interval: str = "1d",
        date_from: Optional[str] = None,
        date_to: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Get temporal distribution of documents
        
        Args:
            interval: Time interval (e.g., "1d", "1w", "1M")
            date_from: Start date
            date_to: End date
            
        Returns:
            Dictionary with distribution data and statistics
        """
        search_body = {
            "size": 0,
            "aggs": {
                "distribution": {
                    "date_histogram": {
                        "field": "date",
                        "calendar_interval": interval,
                        "format": "yyyy-MM-dd",
                        "min_doc_count": 0
                    }
                },
                "stats": {
                    "stats": {
                        "field": "date"
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
        
        buckets = []
        for bucket in response["aggregations"]["distribution"]["buckets"]:
            buckets.append({
                "date": bucket["key_as_string"],
                "timestamp": bucket["key"],
                "count": bucket["doc_count"]
            })
        
        return {
            "buckets": buckets,
            "stats": response["aggregations"]["stats"],
            "total": len(buckets)
        }
    
    def get_georeference_statistics(self, size: int = 10) -> Dict[str, any]:
        """
        Get statistics about georeferences
        
        Args:
            size: Number of top georeferences to return
            
        Returns:
            Dictionary with georeference statistics
        """
        search_body = {
            "size": 0,
            "aggs": {
                "top_georeferences": {
                    "terms": {
                        "field": "georeferences",
                        "size": size
                    }
                },
                "unique_georeferences": {
                    "cardinality": {
                        "field": "georeferences"
                    }
                }
            }
        }
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        top_georeferences = []
        for bucket in response["aggregations"]["top_georeferences"]["buckets"]:
            top_georeferences.append({
                "name": bucket["key"],
                "count": bucket["doc_count"]
            })
        
        return {
            "top_georeferences": top_georeferences,
            "unique_count": response["aggregations"]["unique_georeferences"]["value"],
            "total_top": len(top_georeferences)
        }
    
    def get_author_statistics(self, size: int = 10) -> Dict[str, any]:
        """
        Get statistics about authors
        
        Args:
            size: Number of top authors to return
            
        Returns:
            Dictionary with author statistics
        """
        search_body = {
            "size": 0,
            "aggs": {
                "authors": {
                    "nested": {
                        "path": "authors"
                    },
                    "aggs": {
                        "top_authors": {
                            "terms": {
                                "script": {
                                    "source": "doc['authors.first_name.keyword'].value + ' ' + doc['authors.last_name.keyword'].value"
                                },
                                "size": size
                            }
                        }
                    }
                }
            }
        }
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        top_authors = []
        for bucket in response["aggregations"]["authors"]["top_authors"]["buckets"]:
            top_authors.append({
                "name": bucket["key"],
                "document_count": bucket["doc_count"]
            })
        
        return {
            "top_authors": top_authors,
            "total": len(top_authors)
        }
    
    def get_overview(self) -> Dict[str, any]:
        """
        Get an overview of the entire index
        
        Returns:
            Dictionary with overview statistics
        """
        # Get total count
        count_response = self.es.count(index=self.index_name)
        total_documents = count_response["count"]
        
        # Get date range
        search_body = {
            "size": 0,
            "aggs": {
                "date_stats": {
                    "stats": {
                        "field": "date"
                    }
                },
                "unique_georeferences": {
                    "cardinality": {
                        "field": "georeferences"
                    }
                },
                "documents_with_geopoint": {
                    "filter": {
                        "exists": {
                            "field": "geopoint"
                        }
                    }
                }
            }
        }
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        return {
            "total_documents": total_documents,
            "date_range": {
                "min": response["aggregations"]["date_stats"].get("min_as_string"),
                "max": response["aggregations"]["date_stats"].get("max_as_string")
            },
            "unique_georeferences": response["aggregations"]["unique_georeferences"]["value"],
            "documents_with_geopoint": response["aggregations"]["documents_with_geopoint"]["doc_count"]
        }
    
    def search_by_date_range(
        self,
        date_from: str,
        date_to: str,
        size: int = 100
    ) -> List[Dict[str, any]]:
        """
        Search documents within a date range
        
        Args:
            date_from: Start date
            date_to: End date
            size: Maximum number of results
            
        Returns:
            List of documents
        """
        search_body = {
            "size": size,
            "query": {
                "range": {
                    "date": {
                        "gte": date_from,
                        "lte": date_to
                    }
                }
            },
            "sort": [
                {"date": {"order": "desc"}}
            ]
        }
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        results = []
        for hit in response["hits"]["hits"]:
            results.append({
                "id": hit["_id"],
                **hit["_source"]
            })
        
        return results
    
    def search_by_location(
        self,
        lat: float,
        lon: float,
        distance: str = "100km",
        size: int = 100
    ) -> List[Dict[str, any]]:
        """
        Search documents near a location
        
        Args:
            lat: Latitude
            lon: Longitude
            distance: Search radius (e.g., "100km")
            size: Maximum number of results
            
        Returns:
            List of documents with distances
        """
        search_body = {
            "size": size,
            "query": {
                "geo_distance": {
                    "distance": distance,
                    "geopoint": {
                        "lat": lat,
                        "lon": lon
                    }
                }
            },
            "sort": [
                {
                    "_geo_distance": {
                        "geopoint": {
                            "lat": lat,
                            "lon": lon
                        },
                        "order": "asc",
                        "unit": "km"
                    }
                }
            ]
        }
        
        response = self.es.search(index=self.index_name, body=search_body)
        
        results = []
        for hit in response["hits"]["hits"]:
            result = {
                "id": hit["_id"],
                **hit["_source"]
            }
            
            if "sort" in hit:
                result["distance_km"] = hit["sort"][0]
            
            results.append(result)
        
        return results


if __name__ == "__main__":
    # Test analytics
    analytics = Analytics()
    
    print("Index Overview:")
    overview = analytics.get_overview()
    print(f"  Total Documents: {overview['total_documents']}")
    print(f"  Date Range: {overview['date_range']['min']} to {overview['date_range']['max']}")
    print(f"  Unique Georeferences: {overview['unique_georeferences']}")
    
    print("\nTop Georeferences:")
    geo_stats = analytics.get_georeference_statistics()
    for geo in geo_stats['top_georeferences']:
        print(f"  - {geo['name']}: {geo['count']}")
