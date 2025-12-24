"""
Utility script to manage the Elasticsearch index
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from indexing.document_processor import DocumentProcessor
from search.query_engine import QueryEngine
from search.analytics import Analytics


def main():
    processor = DocumentProcessor()
    query_engine = QueryEngine()
    analytics = Analytics()
    
    print("=" * 60)
    print("Smart Document Retrieval System - Index Manager")
    print("=" * 60)
    
    while True:
        print("\nOptions:")
        print("1. Index sample documents")
        print("2. Delete index")
        print("3. View index statistics")
        print("4. Test search")
        print("5. View top georeferences")
        print("6. Exit")
        
        choice = input("\nEnter your choice (1-6): ").strip()
        
        if choice == "1":
            print("\nIndexing sample documents...")
            try:
                result = processor.index_from_json_file('data/sample_documents/documents.json')
                print(f"✓ Successfully indexed {result['success']} documents")
                if result['failed'] > 0:
                    print(f"✗ Failed to index {result['failed']} documents")
            except Exception as e:
                print(f"✗ Error: {e}")
        
        elif choice == "2":
            confirm = input("Are you sure you want to delete the index? (yes/no): ").strip().lower()
            if confirm == "yes":
                processor.delete_index()
                processor._create_index()
                print("✓ Index deleted and recreated")
            else:
                print("Cancelled")
        
        elif choice == "3":
            try:
                stats = query_engine.get_index_stats()
                overview = analytics.get_overview()
                print(f"\nIndex Statistics:")
                print(f"  Total Documents: {stats['total_documents']}")
                print(f"  Unique Georeferences: {overview['unique_georeferences']}")
                print(f"  Documents with Geopoint: {overview['documents_with_geopoint']}")
                if overview['date_range']['min']:
                    print(f"  Date Range: {overview['date_range']['min']} to {overview['date_range']['max']}")
            except Exception as e:
                print(f"✗ Error: {e}")
        
        elif choice == "4":
            query = input("Enter search query: ").strip()
            if query:
                try:
                    results = query_engine.search(query)
                    print(f"\nFound {len(results)} results:")
                    for i, result in enumerate(results[:5], 1):
                        print(f"\n{i}. {result['title']}")
                        print(f"   Score: {result['score']:.2f}")
                        if result.get('date'):
                            print(f"   Date: {result['date']}")
                except Exception as e:
                    print(f"✗ Error: {e}")
        
        elif choice == "5":
            try:
                results = query_engine.get_top_georeferences(10)
                print("\nTop 10 Georeferences:")
                for i, item in enumerate(results, 1):
                    print(f"{i}. {item['georeference']}: {item['count']} mentions")
            except Exception as e:
                print(f"✗ Error: {e}")
        
        elif choice == "6":
            print("\nGoodbye!")
            break
        
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
