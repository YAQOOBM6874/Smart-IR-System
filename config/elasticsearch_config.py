"""
Elasticsearch Configuration
Defines index mappings and settings for the Smart Document Retrieval System
"""

INDEX_NAME = "smart_documents"


INDEX_SETTINGS = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "analysis": {
            "analyzer": {
                "autocomplete_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "autocomplete_filter"
                    ]
                },
                "autocomplete_search_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase"
                    ]
                },
                "content_analyzer": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "stop",
                        "length_filter",
                        "stemmer"
                    ],
                    "char_filter": [
                        "html_strip"
                    ]
                }
            },
            "filter": {
                "autocomplete_filter": {
                    "type": "edge_ngram",
                    "min_gram": 3,
                    "max_gram": 20
                },
                "length_filter": {
                    "type": "length",
                    "min": 3
                },
                "stemmer": {
                    "type": "stemmer",
                    "language": "english"
                }
            }
        }
    },
    "mappings": {
        "properties": {
            "title": {
                "type": "text",
                "analyzer": "autocomplete_analyzer",
                "search_analyzer": "autocomplete_search_analyzer",
                "similarity": "BM25",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    },
                    "standard": {
                        "type": "text",
                        "analyzer": "standard"
                    }
                }
            },
            "content": {
                "type": "text",
                "analyzer": "content_analyzer",
                "similarity": "BM25",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            },
            "authors": {
                "type": "nested",
                "properties": {
                    "first_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "last_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword"
                            }
                        }
                    },
                    "email": {
                        "type": "keyword"
                    }
                }
            },
            "date": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||epoch_millis"
            },
            "geopoint": {
                "type": "geo_point"
            },
            "temporal_expressions": {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword"
                    }
                }
            },
            "georeferences": {
                "type": "keyword"
            },
            "extracted_dates": {
                "type": "date",
                "format": "yyyy-MM-dd'T'HH:mm:ss||yyyy-MM-dd||epoch_millis"
            },
            "extracted_locations": {
                "type": "nested",
                "properties": {
                    "name": {
                        "type": "keyword"
                    },
                    "coordinates": {
                        "type": "geo_point"
                    }
                }
            },
            "content_vector": {
                "type": "dense_vector",
                "dims": 384,
                "index": True,
                "similarity": "cosine"
            }
        }
    }
}


def get_elasticsearch_config():
    """
    Returns the Elasticsearch configuration dictionary
    """
    return {
        "index_name": INDEX_NAME,
        "settings": INDEX_SETTINGS
    }
