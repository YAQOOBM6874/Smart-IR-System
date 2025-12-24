"""
Main Entry Point
Run this file to start the Smart Document Retrieval System
"""

import sys
import os

# Add the project root to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from web.app import app

if __name__ == '__main__':
    print("=" * 60)
    print("Smart Document Retrieval System")
    print("=" * 60)
    print("\nStarting web server...")
    print("Access the application at: http://localhost:5000")
    print("\nMake sure Elasticsearch is running on http://localhost:9200")
    print("\nPress Ctrl+C to stop the server")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
