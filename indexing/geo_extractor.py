"""
Georeference Extractor
Extracts place names and geocodes them to coordinates using NER and geocoding API
Uses spaCy for intelligent location detection
"""

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from typing import List, Dict, Optional, Tuple, Set
import time
import re

# Try to import spaCy for intelligent NER
try:
    import spacy
    SPACY_AVAILABLE = True
    try:
        nlp = spacy.load("en_core_web_sm")
    except OSError:
        print("Warning: spaCy model 'en_core_web_sm' not found. Run: python -m spacy download en_core_web_sm")
        nlp = None
        SPACY_AVAILABLE = False
except ImportError:
    print("Warning: spaCy not installed. Using fallback regex patterns.")
    SPACY_AVAILABLE = False
    nlp = None


class GeoExtractor:
    """
    Extracts georeferences from text and geocodes them to coordinates
    """
    
    def __init__(self):
        """
        Initialize the geo extractor with geocoder
        """
        # Initialize geocoder with a user agent
        self.geocoder = Nominatim(user_agent="smart_document_retrieval_system")
        self.geocode_cache = {}  # Cache to avoid repeated geocoding
        
    def extract_georeferences(self, text: str) -> List[str]:
        """
        Extract place names from text using spaCy NER
        
        Args:
            text: Input text to extract georeferences from
            
        Returns:
            List of place name strings
        """
        if not text:
            return []
        
        georeferences: Set[str] = set()
        
        # Use spaCy NER for high accuracy (GPE, LOC, FAC)
        if SPACY_AVAILABLE and nlp is not None:
            doc = nlp(text)
            for ent in doc.ents:
                # Extract GPE, LOC, FAC but explicitly exclude 'Untitled'
                if ent.label_ in ['GPE', 'LOC', 'FAC'] and ent.text.lower() != 'untitled':
                    georeferences.add(ent.text)
        
        # Convert to list and sort by length (longer names first - more specific)
        georeferences_list = sorted(list(georeferences), key=len, reverse=True)
        
        return georeferences_list
    
    def geocode_location(self, location_name: str, context: Optional[str] = None) -> Optional[Dict[str, any]]:
        """
        Geocode a location name to coordinates (latitude, longitude)
        
        Args:
            location_name: Name of the location
            context: Optional context (e.g., country name) to improve accuracy
            
        Returns:
            Dictionary with 'name', 'lat', 'lon', 'formatted_address' or None
        """
        # Expand common Reuters abbreviations for better geocoding
        abbreviations = {
            'usa': 'United States',
            'uk': 'United Kingdom',
            'west-germany': 'Germany',
            'ussr': 'Russia',
            'u.a.e': 'United Arab Emirates'
        }
        
        clean_name = location_name.lower().strip()
        if clean_name in abbreviations:
            location_name = abbreviations[clean_name]

        # Create a search query with context if provided
        search_query = f"{location_name}, {context}" if context and context.lower() not in location_name.lower() else location_name
        
        # Check cache first
        if search_query in self.geocode_cache:
            coords = self.geocode_cache[search_query]
            if coords:
                return {
                    "name": location_name,
                    "lat": coords[0],
                    "lon": coords[1],
                    "formatted_address": search_query
                }
            return None
            
        try:
            # Call Nominatim API with rate limiting
            time.sleep(0.2)
            location = self.geocoder.geocode(search_query, timeout=10)
            
            # If search with context failed, try without context (only if context was used)
            if not location and context:
                time.sleep(0.2)
                location = self.geocoder.geocode(location_name, timeout=10)
                search_query = location_name # Update query name for cache
            
            if location:
                coords = (location.latitude, location.longitude)
                self.geocode_cache[search_query] = coords
                return {
                    "name": location_name,
                    "lat": coords[0],
                    "lon": coords[1],
                    "formatted_address": location.address
                }
            
            # Fallback for common places if geocoding fails
            fallback_coords = {
                'New York': (40.7128, -74.0060),
                'London': (51.5074, -0.1278),
                'Paris': (48.8566, 2.3522),
                'Tokyo': (35.6762, 139.6503),
                'Berlin': (52.5200, 13.4050),
                'Rome': (41.9028, 12.4964),
                'Madrid': (40.4168, -3.7038),
                'Beijing': (39.9042, 116.4074),
                'Moscow': (55.7558, 37.6173),
                'Sydney': (-33.8688, 151.2093),
                'Boston': (42.3601, -71.0589),
                'Singapore': (1.3521, 103.8198),
                'Zurich': (47.3769, 8.5417),
                'Cambridge': (52.2053, 0.1218)
            }
            
            for place,coords in fallback_coords.items():
                if place.lower() in location_name.lower():
                    self.geocode_cache[search_query] = coords
                    return {
                        "name": location_name,
                        "lat": coords[0],
                        "lon": coords[1],
                        "formatted_address": location_name
                    }
                    
            return None
            
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            print(f"Warning: Geocoding failed for '{search_query}': {e}")
            return None
        except Exception as e:
            print(f"Warning: Error geocoding '{search_query}': {e}")
            return None
    
    def extract_and_geocode(self, text: str) -> Dict[str, any]:
        """
        Extract georeferences and geocode them with context awareness
        
        Args:
            text: Input text
            
        Returns:
            Dictionary containing georeferences and geocoded locations
        """
        georeferences = self.extract_georeferences(text)
        geocoded_locations = []
        
        # Identify "High-level" context (like countries) from the extracted georefs
        # In a real app, you might have a list of countries. 
        # Here we'll treat any GPE that matches a known country or just the most common GPE as context.
        # Simple heuristic: longer GPEs often contain more info, but countries are usually short.
        # Let's use the first GPE found as a potential context for subsequent ones if it looks like a country.
        
        possible_context = None
        # Use spaCy to find the most likely "Country" or "State" entity
        if SPACY_AVAILABLE and nlp is not None:
            doc = nlp(text)
            for ent in doc.ents:
                if ent.label_ == 'GPE':
                    # Heuristic: If it's a known country or just a common GPE found early
                    # For this IR project, we'll just take the first GPE as a candidate context
                    possible_context = ent.text
                    break

        for georef in georeferences:
            # If the georef is the same as context, don't use context
            current_context = possible_context if georef != possible_context else None
            
            geocoded = self.geocode_location(georef, context=current_context)
            if geocoded:
                geocoded_locations.append(geocoded)
        
        # Primary location is the first one found
        primary_location = geocoded_locations[0] if geocoded_locations else None
        
        return {
            "georeferences": georeferences,
            "geocoded_locations": geocoded_locations,
            "primary_location": primary_location
        }
    
    def approximate_geopoint(self, text: str, default_location: Optional[Tuple[float, float]] = None) -> Optional[Dict[str, float]]:
        """
        Approximate a geopoint from text, returning the first geocoded location
        or a default location if none found
        
        Args:
            text: Input text
            default_location: Default (lat, lon) tuple to return if no locations found
            
        Returns:
            Dictionary with 'lat' and 'lon' or None
        """
        result = self.extract_and_geocode(text)
        
        if result["primary_location"]:
            return {
                "lat": result["primary_location"]["lat"],
                "lon": result["primary_location"]["lon"]
            }
        
        if default_location:
            return {
                "lat": default_location[0],
                "lon": default_location[1]
            }
        
        return None


if __name__ == "__main__":
    # Test the geo extractor
    extractor = GeoExtractor()
    
    test_texts = [
        "The conference was held in New York City at the Empire State Building.",
        "Researchers from London, Paris, and Tokyo collaborated on this project.",
        "The study focused on climate change in the Amazon rainforest.",
        "No locations in this text."
    ]
    
    for text in test_texts:
        print(f"\nText: {text}")
        result = extractor.extract_and_geocode(text)
        print(f"Georeferences: {result['georeferences']}")
        print(f"Geocoded Locations: {len(result['geocoded_locations'])} found")
        if result['primary_location']:
            print(f"Primary Location: {result['primary_location']['name']} at ({result['primary_location']['lat']}, {result['primary_location']['lon']})")
