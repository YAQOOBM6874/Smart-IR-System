"""
Reuters-21578 SGM Parser
Parses Reuters-21578 SGML files and extracts structured data
"""

import re
from typing import List, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup


class ReutersParser:
    """
    Parser for Reuters-21578 SGML format files
    """
    
    def __init__(self):
        """Initialize the Reuters parser"""
        pass
    
    def parse_file(self, file_path: str) -> List[Dict[str, any]]:
        """
        Parse a Reuters SGM file and extract all documents
        
        Args:
            file_path: Path to the .sgm file
            
        Returns:
            List of document dictionaries
        """
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Split by REUTERS tags
        documents = []
        reuters_pattern = r'<REUTERS[^>]*>(.*?)</REUTERS>'
        matches = re.findall(reuters_pattern, content, re.DOTALL)
        
        for match in matches:
            doc = self._parse_document(match)
            if doc:
                documents.append(doc)
        
        return documents
    
    def _parse_document(self, reuters_content: str) -> Optional[Dict[str, any]]:
        """
        Parse a single REUTERS document
        
        Args:
            reuters_content: Content between REUTERS tags
            
        Returns:
            Document dictionary or None if parsing fails
        """
        try:
            # Extract date
            date = self._extract_tag_content(reuters_content, 'DATE')
            parsed_date = self._parse_date(date) if date else None
            
            # Extract title
            title = self._extract_tag_content(reuters_content, 'TITLE')
            
            # Extract body
            body = self._extract_tag_content(reuters_content, 'BODY')
            if not body:
                body = ""
                
            # Improved Title Handling: If no title, try to use part of the body or dateline
            if not title:
                if body:
                    # Take first sentence or first 60 chars as title
                    snippet = body.split('.')[0][:60].strip()
                    title = snippet if snippet else "Reuters News"
                else:
                    dateline = self._extract_tag_content(reuters_content, 'DATELINE')
                    title = dateline.strip() if dateline else "Reuters News Update"
            
            # Extract dateline (contains location info)
            dateline = self._extract_tag_content(reuters_content, 'DATELINE')
            
            # Extract topics
            topics = self._extract_list_content(reuters_content, 'TOPICS')
            
            # Extract places
            places = self._extract_list_content(reuters_content, 'PLACES')
            
            # Extract people
            people = self._extract_list_content(reuters_content, 'PEOPLE')
            
            # Extract orgs
            orgs = self._extract_list_content(reuters_content, 'ORGS')
            
            # Combine title and body for full content
            content = f"{title}\n\n{body}".strip()
            
            # Build document with structured metadata
            # Use Reuters' structured data directly for better accuracy
            doc = {
                'title': title.strip(),
                'content': content,
                'date': parsed_date.isoformat() if parsed_date else None,
                'topics': topics,
                'places': places,
                'people': people,
                'organizations': orgs,
                'dateline': dateline,
                # Pass structured metadata for better extraction
                'metadata': {
                    'reuters_places': places,  # Use as georeferences
                    'reuters_date': parsed_date.isoformat() if parsed_date else None,
                    'reuters_topics': topics
                }
            }
            
            return doc
            
        except Exception as e:
            print(f"Error parsing document: {e}")
            return None
    
    def _extract_tag_content(self, text: str, tag: str) -> Optional[str]:
        """
        Extract content between XML/SGML tags
        
        Args:
            text: Text to search in
            tag: Tag name (without < >)
            
        Returns:
            Content between tags or None
        """
        pattern = f'<{tag}>(.*?)</{tag}>'
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            content = match.group(1).strip()
            # Remove any remaining tags
            content = re.sub(r'<[^>]+>', '', content)
            return content
        return None
    
    def _extract_list_content(self, text: str, tag: str) -> List[str]:
        """
        Extract list items from tags like TOPICS, PLACES, etc.
        These contain <D>item</D> elements
        
        Args:
            text: Text to search in
            tag: Tag name (TOPICS, PLACES, etc.)
            
        Returns:
            List of items
        """
        # First extract the content between the main tags
        pattern = f'<{tag}>(.*?)</{tag}>'
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        
        if not match:
            return []
        
        content = match.group(1)
        
        # Now extract all <D> items
        items = re.findall(r'<D>(.*?)</D>', content, re.IGNORECASE)
        return [item.strip() for item in items if item.strip()]
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """
        Parse Reuters date format: "26-FEB-1987 15:01:01.79"
        
        Args:
            date_str: Date string from Reuters
            
        Returns:
            datetime object or None
        """
        if not date_str:
            return None
        
        try:
            # Reuters format: DD-MMM-YYYY HH:MM:SS.ss
            # Remove milliseconds if present
            date_str = re.sub(r'\.\d+$', '', date_str.strip())
            
            # Try to parse
            return datetime.strptime(date_str, '%d-%b-%Y %H:%M:%S')
        except:
            try:
                # Try without time
                return datetime.strptime(date_str[:11], '%d-%b-%Y')
            except:
                return None


if __name__ == '__main__':
    # Test the parser
    import os
    
    parser = ReutersParser()
    
    # Parse first file
    test_file = os.path.join('archive', 'reut2-000.sgm')
    
    if os.path.exists(test_file):
        print(f"Parsing {test_file}...")
        documents = parser.parse_file(test_file)
        
        print(f"\nFound {len(documents)} documents")
        
        # Show first document
        if documents:
            doc = documents[0]
            print("\n" + "="*60)
            print("SAMPLE DOCUMENT:")
            print("="*60)
            print(f"Title: {doc['title']}")
            print(f"Date: {doc['date']}")
            print(f"Topics: {doc['topics']}")
            print(f"Places: {doc['places']}")
            print(f"Content (first 200 chars): {doc['content'][:200]}...")
            print("="*60)
    else:
        print(f"File not found: {test_file}")
