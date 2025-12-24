"""
Temporal Expression Extractor
Extracts dates and temporal expressions from document text using spaCy NER and dateparser
"""

import dateparser
from datetime import datetime
from typing import List, Dict, Optional
import re

# Try to import spaCy for intelligent NER
try:
    import spacy
    SPACY_AVAILABLE = True
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        nlp = None
        SPACY_AVAILABLE = False
except ImportError:
    SPACY_AVAILABLE = False
    nlp = None


class TemporalExtractor:
    """
    Extracts temporal expressions from text using spaCy Named Entity Recognition (NER)
    """
    
    def __init__(self):
        """
        Initialize the temporal extractor
        """
        pass
    
    def extract_temporal_expressions(self, text: str) -> Dict[str, any]:
        """
        Extract temporal expressions from text using spaCy NER
        
        Args:
            text: Input text to extract temporal expressions from
            
        Returns:
            Dictionary containing:
                - temporal_expressions: List of temporal expression strings
                - parsed_dates: List of parsed datetime objects
                - most_recent_date: The most recent date found (or None)
        """
        if not text:
            return {
                "temporal_expressions": [],
                "parsed_dates": [],
                "most_recent_date": None
            }
        
        temporal_expressions = []
        
        # Use spaCy NER for intelligent temporal extraction
        if SPACY_AVAILABLE and nlp is not None:
            doc = nlp(text)
            for ent in doc.ents:
                # Extract DATE and TIME entities
                if ent.label_ in ['DATE', 'TIME']:
                    temporal_expressions.append(ent.text)
        else:
            # Fallback to a very simple date regex if spaCy fails
            date_pattern = r'\b(?:\d{1,4}[-/]\d{1,2}[-/]\d{1,4})\b'
            temporal_expressions = re.findall(date_pattern, text)
        
        # Remove duplicates while preserving order
        temporal_expressions = list(dict.fromkeys(temporal_expressions))
        
        # Parse dates into standard format
        parsed_dates = []
        for expr in temporal_expressions:
            # dateparser is great for converting 'yesterday' or 'March 15' to objects
            parsed = dateparser.parse(expr)
            if parsed:
                parsed_dates.append(parsed)
        
        # Find most recent date for metadata prioritization
        most_recent_date = None
        if parsed_dates:
            most_recent_date = max(parsed_dates)
        
        return {
            "temporal_expressions": temporal_expressions,
            "parsed_dates": parsed_dates,
            "most_recent_date": most_recent_date
        }
    
    def approximate_date(self, text: str, default_date: Optional[datetime] = None) -> Optional[datetime]:
        """
        Approximate a date from text, returning the most recent date found
        or a default date if none found
        """
        result = self.extract_temporal_expressions(text)
        
        if result["most_recent_date"]:
            return result["most_recent_date"]
        
        return default_date


if __name__ == "__main__":
    # Test the temporal extractor
    extractor = TemporalExtractor()
    
    test_texts = [
        "The conference was held on March 15, 2023 in New York.",
        "Published on 2024-01-20. Last updated yesterday.",
        "The event will take place next week, specifically on Friday, December 1st, 2023.",
        "The price increased by 5 pct in 1987."
    ]
    
    for text in test_texts:
        print(f"\nText: {text}")
        result = extractor.extract_temporal_expressions(text)
        print(f"Temporal Expressions: {result['temporal_expressions']}")
        print(f"Parsed Dates: {len(result['parsed_dates'])} found")
        if result['most_recent_date']:
            print(f"Most Recent: {result['most_recent_date']}")
