"""
Reuters-21578 Indexer (Root Context)
Indexes all Reuters-21578 documents into Elasticsearch
"""

import os
import glob
from indexing.reuters_parser import ReutersParser
from indexing.document_processor import DocumentProcessor
from tqdm import tqdm


def index_reuters_dataset(archive_dir='archive', delete_existing=True):
    """
    Index all Reuters-21578 documents
    """
    print("=" * 60)
    print("Reuters-21578 Dataset Indexer (Enhanced with NLP)")
    print("=" * 60)
    
    # Initialize parser and processor
    parser = ReutersParser()
    processor = DocumentProcessor()
    
    # Delete existing index if requested
    if delete_existing:
        print("\n[1/4] Deleting existing index...")
        processor.delete_index()
        processor._create_index()
        print("✓ Index recreated")
    
    # Find all SGM files
    print("\n[2/4] Finding Reuters files...")
    sgm_files = sorted(glob.glob(os.path.join(archive_dir, 'reut2-*.sgm')))
    
    if not sgm_files:
        print(f"✗ No .sgm files found in {archive_dir}")
        return
    
    print(f"✓ Found {len(sgm_files)} files")
    
    # Parse all files
    print("\n[3/4] Parsing documents with NLP improvements...")
    all_documents = []
    
    for sgm_file in tqdm(sgm_files, desc="Parsing files"):
        try:
            docs = parser.parse_file(sgm_file)
            all_documents.extend(docs)
        except Exception as e:
            print(f"\n✗ Error parsing {sgm_file}: {e}")
    
    print(f"✓ Parsed {len(all_documents)} documents")
    
    # Index documents in batches
    print("\n[4/4] Indexing documents (Georeference & Temporal Analysis)...")
    batch_size = 50 # Smaller batch for stability
    total_indexed = 0
    total_failed = 0
    
    for i in tqdm(range(0, len(all_documents), batch_size), desc="Indexing batches"):
        batch = all_documents[i:i+batch_size]
        
        try:
            result = processor.index_documents_bulk(batch)
            total_indexed += result['success']
            total_failed += result['failed']
        except Exception as e:
            print(f"\n✗ Error indexing batch {i//batch_size + 1}: {e}")
            total_failed += len(batch)
    
    # Summary
    print("\n" + "=" * 60)
    print("Indexing Complete!")
    print("=" * 60)
    print(f"✓ Successfully indexed: {total_indexed} documents")
    print(f"📊 Total processed: {len(all_documents)}")
    print("=" * 60)
    print("\n🎉 Everything is ready! Run: python main.py")


if __name__ == '__main__':
    index_reuters_dataset()
