# Smart Document Retrieval System - Configuration

# Elasticsearch Configuration
ELASTICSEARCH_HOST = "http://localhost:9200"
INDEX_NAME = "smart_documents"

# Flask Configuration
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5000
FLASK_DEBUG = True

# Search Configuration
DEFAULT_SEARCH_SIZE = 10
AUTOCOMPLETE_MIN_CHARS = 3
AUTOCOMPLETE_SIZE = 10

# Analytics Configuration
TOP_GEOREFERENCES_SIZE = 10
TEMPORAL_DISTRIBUTION_INTERVAL = "1d"

# Geocoding Configuration
GEOCODING_DELAY = 0.5  # seconds between requests
GEOCODING_TIMEOUT = 10  # seconds

# Date Configuration
DEFAULT_DATE_FORMAT = "%Y-%m-%d"
