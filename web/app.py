"""
Flask Web Application
Provides a web interface for the Smart Document Retrieval System
"""

from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
from search.query_engine import QueryEngine
from search.analytics import Analytics
from indexing.document_processor import DocumentProcessor
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)

# Initialize components
query_engine = QueryEngine()
analytics = Analytics()
document_processor = DocumentProcessor()


@app.route('/')
def index():
    """
    Render the main page
    """
    return render_template('index.html')


@app.route('/api/autocomplete', methods=['GET'])
def autocomplete():
    """
    Autocomplete API endpoint
    """
    query = request.args.get('q', '')
    
    if len(query) < 3:
        return jsonify([])
    
    results = query_engine.autocomplete(query)
    return jsonify(results)


@app.route('/api/search', methods=['POST'])
def search():
    """
    Search API endpoint
    """
    data = request.json
    
    query = data.get('query', '')
    temporal_expression = data.get('temporal_expression')
    georeference = data.get('georeference')
    date_from = data.get('date_from')
    date_to = data.get('date_to')
    location = data.get('location')  # [lat, lon]
    distance = data.get('distance', '100km')
    size = data.get('size', 10)
    semantic_weight = data.get('semantic_weight', 0.5)
    
    # Convert location to tuple if provided
    if location and isinstance(location, list) and len(location) == 2:
        location = tuple(location)
    else:
        location = None
    
    results = query_engine.search(
        query=query,
        temporal_expression=temporal_expression,
        georeference=georeference,
        date_from=date_from,
        date_to=date_to,
        location=location,
        distance=distance,
        size=size,
        semantic_weight=semantic_weight
    )
    
    return jsonify(results)


@app.route('/api/analytics/georeferences', methods=['GET'])
def top_georeferences():
    """
    Get top georeferences
    """
    size = request.args.get('size', 10, type=int)
    results = query_engine.get_top_georeferences(size)
    return jsonify(results)


@app.route('/api/analytics/distribution', methods=['GET'])
def document_distribution():
    """
    Get document distribution over time
    """
    interval = request.args.get('interval', '1d')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    
    results = query_engine.get_document_distribution_over_time(
        interval=interval,
        date_from=date_from,
        date_to=date_to
    )
    
    return jsonify(results)


@app.route('/api/analytics/overview', methods=['GET'])
def overview():
    """
    Get index overview
    """
    results = analytics.get_overview()
    return jsonify(results)


@app.route('/api/analytics/geostats', methods=['GET'])
def geostats():
    """
    Get georeference statistics
    """
    size = request.args.get('size', 10, type=int)
    results = analytics.get_georeference_statistics(size)
    return jsonify(results)


@app.route('/api/analytics/temporal', methods=['GET'])
def temporal_distribution():
    """
    Get temporal distribution
    """
    interval = request.args.get('interval', '1d')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    
    results = analytics.get_temporal_distribution(
        interval=interval,
        date_from=date_from,
        date_to=date_to
    )
    
    return jsonify(results)


@app.route('/api/document/<doc_id>', methods=['GET'])
def get_document(doc_id):
    """
    Get a specific document by ID
    """
    result = query_engine.get_document_by_id(doc_id)
    
    if result:
        return jsonify(result)
    else:
        return jsonify({"error": "Document not found"}), 404


@app.route('/api/index/stats', methods=['GET'])
def index_stats():
    """
    Get index statistics
    """
    results = query_engine.get_index_stats()
    return jsonify(results)


@app.route('/api/index/sample', methods=['POST'])
def index_sample_documents():
    """
    Index sample documents
    """
    try:
        sample_file = os.path.join('data', 'sample_documents', 'documents.json')
        result = document_processor.index_from_json_file(sample_file)
        return jsonify({
            "success": True,
            "message": f"Indexed {result['success']} documents successfully",
            "stats": result
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
