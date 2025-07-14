import requests
import csv
import json
import time
from datetime import datetime
import logging
from collections import defaultdict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PhishDataExtractor:
    def __init__(self, api_key="A59D71B642DA223E6F61"):
        """
        Initialize the Phish.net API extractor
        
        Args:
            api_key (str): Your phish.net API key (optional but recommended for higher rate limits)
        """
        self.base_url = "https://api.phish.net/v5"
        self.api_key = api_key
        self.headers = {
            'User-Agent': 'PhishDataExtractor/1.0',
            'Accept': 'application/json'
        }
        if api_key:
            self.headers['Authorization'] = f'Bearer {api_key}'
        
        # Batch processing settings
        self.batch_size = 50  # Process shows in batches of 50
        self.request_delay = 0.5  # 0.5 second delay between requests
        self.batch_delay = 2.0  # 2 second delay between batches
        
        # Wide format settings
        self.all_songs = set()  # Track all unique songs across all shows
        self.song_features = {}  # Track song features for wide format
    
    def make_request(self, endpoint, params=None, retry_count=3):
        """Make a request to the phish.net API with error handling and rate limiting"""
        if params is None:
            params = {}
        
        if self.api_key:
            params['apikey'] = self.api_key
        else:
            logger.error("API key is required for phish.net API access")
            return None
            
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(retry_count):
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                
                # Handle rate limiting
                if response.status_code == 429:
                    wait_time = 5 * (attempt + 1)  # Exponential backoff
                    logger.warning(f"Rate limited. Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                
                response.raise_for_status()
                
                # Rate limiting - be respectful to the API
                time.sleep(self.request_delay)
                
                data = response.json()
                
                # Check for API errors
                if data.get('error') and data.get('error') != False:
                    logger.error(f"API returned error: {data.get('error')} - {data.get('error_message', 'No message')}")
                    return None
                
                return data
                
            except requests.exceptions.RequestException as e:
                if attempt < retry_count - 1:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{retry_count}): {e}")
                    time.sleep(2 * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    logger.error(f"API request failed for {url} after {retry_count} attempts: {e}")
                    return None
            except json.JSONDecodeError as e:
                logger.error(f"Failed to decode JSON response: {e}")
                return None
        
        return None
    
    def get_all_shows(self, start_year=None, end_year=None):
        """Get all Phish shows from the API by year with better batching"""
        logger.info("Fetching all Phish shows by year...")
        
        all_shows = []
        
        # Set default year range
        if start_year is None:
            start_year = 1983
        if end_year is None:
            end_year = datetime.now().year
        
        # Process years in smaller batches to avoid overwhelming the API
        year_batch_size = 5  # Process 5 years at a time
        
        for year_start in range(start_year, end_year + 1, year_batch_size):
            year_end = min(year_start + year_batch_size - 1, end_year)
            
            logger.info(f"Processing years {year_start} to {year_end}...")
            
            for year in range(year_start, year_end + 1):
                logger.info(f"Fetching shows for year {year}...")
                
                # Use the proper endpoint format
                endpoint = f'shows/showyear/{year}.json'
                params = {'order_by': 'showdate'}
                
                response = self.make_request(endpoint, params)
                if response and 'data' in response and response.get('error') == False:
                    year_shows = response['data']
                    if year_shows:
                        # Filter for only Phish shows
                        phish_shows = [show for show in year_shows if show.get('artist_name', '').lower() == 'phish']
                        all_shows.extend(phish_shows)
                        logger.info(f"Found {len(phish_shows)} Phish shows for {year} (filtered from {len(year_shows)} total)")
                    else:
                        logger.info(f"No shows found for {year}")
                elif response and response.get('error'):
                    logger.warning(f"API error for year {year}: {response.get('error_message', 'Unknown error')}")
                else:
                    logger.warning(f"Failed to fetch data for year {year}")
            
            # Longer pause between year batches
            if year_end < end_year:
                logger.info(f"Completed years {year_start}-{year_end}, pausing before next batch...")
                time.sleep(self.batch_delay)
        
        logger.info(f"Total Phish shows fetched: {len(all_shows)}")
        return all_shows
    
    def get_show_setlist(self, show_date):
        """Get detailed setlist for a specific show by date"""
        # Use the setlists endpoint for detailed setlist data
        endpoint = f'setlists/showdate/{show_date}.json'
        response = self.make_request(endpoint)
        if response and 'data' in response and response.get('error') == False:
            return response['data']
        return None
    
    def format_setlist(self, setlist_data):
        """Format setlist data into a readable string"""
        if not setlist_data:
            return ""
        
        formatted_sets = []
        
        for set_info in setlist_data:
            set_name = set_info.get('set_name', 'Unknown Set')
            songs = set_info.get('songs', [])
            
            if songs:
                song_list = []
                for song in songs:
                    song_name = song.get('song_name', 'Unknown Song')
                    # Add segue information if available
                    if song.get('segue'):
                        song_name += f" > "
                    song_list.append(song_name)
                
                formatted_sets.append(f"{set_name}: {', '.join(song_list)}")
        
        return " | ".join(formatted_sets)
    
    def extract_song_features_from_setlist(self, setlist_data):
        """Extract song features for wide format modeling"""
        song_features = {
            'songs_played': [],
            'set_positions': {},
            'song_counts': defaultdict(int),
            'has_segues': {},
            'set_info': defaultdict(list)
        }
        
        if not setlist_data:
            return song_features
        
        if isinstance(setlist_data, list):
            for entry in setlist_data:
                song_name = entry.get('song', 'Unknown Song')
                set_name = entry.get('set', 'Unknown Set')
                position = entry.get('position', 0)
                segue = entry.get('segue', '')
                
                # Clean song name
                song_name = song_name.strip()
                if song_name and song_name != 'Unknown Song':
                    # Add to global song tracking
                    self.all_songs.add(song_name)
                    
                    # Track for this show
                    song_features['songs_played'].append(song_name)
                    song_features['song_counts'][song_name] += 1
                    song_features['set_positions'][song_name] = position
                    song_features['has_segues'][song_name] = bool(segue and segue.strip())
                    song_features['set_info'][set_name].append(song_name)
        
        return song_features
    
    def extract_all_data(self, start_year=None, end_year=None):
        """Extract all show data including setlists with improved batching"""
        shows = self.get_all_shows(start_year, end_year)
        
        if not shows:
            logger.error("No shows data retrieved")
            return []
        
        enriched_shows = []
        total_shows = len(shows)
        
        # Process shows in batches
        for batch_start in range(0, total_shows, self.batch_size):
            batch_end = min(batch_start + self.batch_size, total_shows)
            batch_shows = shows[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_start//self.batch_size + 1}/{(total_shows + self.batch_size - 1)//self.batch_size}")
            logger.info(f"Shows {batch_start + 1} to {batch_end} of {total_shows}")
            
            for i, show in enumerate(batch_shows):
                show_date = show.get('showdate', '')
                current_show = batch_start + i + 1
                
                logger.info(f"Processing show {current_show}/{total_shows}: {show_date}")
                
                # Verify this is a Phish show
                if show.get('artist_name', '').lower() != 'phish':
                    logger.warning(f"Skipping non-Phish show: {show.get('artist_name', 'Unknown Artist')}")
                    continue
                
                # Get detailed setlist for this show using the date
                detailed_setlist = self.get_show_setlist(show_date)
                setlist_formatted = ""
                song_features = {}
                
                if detailed_setlist:
                    setlist_formatted = self.format_setlist_from_show(detailed_setlist)
                    song_features = self.extract_song_features_from_setlist(detailed_setlist)
                else:
                    logger.warning(f"No setlist data found for {show_date}")
                
                # Create enriched show data
                enriched_show = {
                    'show_id': show.get('showid', ''),
                    'date': show_date,
                    'artist_name': show.get('artist_name', ''),
                    'venue': show.get('venue', ''),
                    'city': show.get('city', ''),
                    'state': show.get('state', ''),
                    'country': show.get('country', ''),
                    'tour_name': show.get('tour_name', ''),
                    'setlist': setlist_formatted,
                    'rating': show.get('rating', ''),
                    'reviews': show.get('reviews', ''),
                    'venue_id': show.get('venueid', ''),
                    'permalink': show.get('permalink', ''),
                    'song_features': song_features
                }
                
                enriched_shows.append(enriched_show)
            
            # Pause between batches to be respectful to the API
            if batch_end < total_shows:
                logger.info(f"Completed batch, pausing {self.batch_delay} seconds before next batch...")
                time.sleep(self.batch_delay)
        
        return enriched_shows
    
    def format_setlist_from_show(self, setlist_data):
        """Format setlist data from setlist response"""
        if not setlist_data:
            return ""
        
        # Handle list of setlist entries
        if isinstance(setlist_data, list):
            if len(setlist_data) == 0:
                return ""
            # Group by set
            sets = {}
            for entry in setlist_data:
                set_name = entry.get('set', 'Unknown Set')
                song_name = entry.get('song', 'Unknown Song')
                position = entry.get('position', 999)
                
                if set_name not in sets:
                    sets[set_name] = []
                
                # Add segue information if available
                segue = entry.get('segue', '')
                if segue and segue.strip():
                    song_name += f" {segue} "
                
                sets[set_name].append((position, song_name))
            
            # Format each set
            formatted_sets = []
            for set_name, songs in sets.items():
                # Sort songs by position
                songs.sort(key=lambda x: x[0])
                song_names = [song[1] for song in songs]
                formatted_sets.append(f"{set_name}: {', '.join(song_names)}")
            
            return " | ".join(formatted_sets)
        
        return ""
    
    def generate_filename(self, start_year=None, end_year=None, file_type='complete'):
        """Generate a filename based on the year range"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if start_year and end_year:
            if start_year == end_year:
                year_str = str(start_year)
            else:
                year_str = f"{start_year}-{end_year}"
        else:
            year_str = "all_years"
        
        return f"phish_shows_{year_str}_{file_type}_{timestamp}.csv"

    def save_to_csv(self, data, filename=None, start_year=None, end_year=None, file_type='complete'):
        """Save the extracted data to a CSV file with automatic filename generation"""
        if not data:
            logger.error("No data to save")
            return
        
        if filename is None:
            filename = self.generate_filename(start_year, end_year, file_type)
        
        logger.info(f"Saving {len(data)} shows to {filename}")
        
        fieldnames = [
            'show_id', 'date', 'artist_name', 'venue', 'city', 'state', 'country',
            'tour_name', 'setlist', 'rating', 'reviews', 'venue_id', 'permalink'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for show in data:
                # Remove song_features from the row for standard CSV
                row = {k: v for k, v in show.items() if k != 'song_features'}
                writer.writerow(row)
        
        logger.info(f"Data successfully saved to {filename}")
        return filename
    
    def save_to_csv_wide_format(self, data, filename=None, start_year=None, end_year=None):
        """Save data in wide format for machine learning modeling"""
        if not data:
            logger.error("No data to save")
            return
        
        if filename is None:
            filename = self.generate_filename(start_year, end_year, 'wide_format')
        
        logger.info(f"Converting {len(data)} shows to wide format for modeling...")
        logger.info(f"Total unique songs found: {len(self.all_songs)}")
        
        # Create wide format data
        wide_data = []
        
        for show in data:
            # Base show information
            wide_row = {
                'show_id': show.get('show_id', ''),
                'date': show.get('date', ''),
                'artist_name': show.get('artist_name', ''),
                'venue': show.get('venue', ''),
                'city': show.get('city', ''),
                'state': show.get('state', ''),
                'country': show.get('country', ''),
                'tour_name': show.get('tour_name', ''),
                'rating': show.get('rating', ''),
                'reviews': show.get('reviews', ''),
                'venue_id': show.get('venue_id', ''),
                'permalink': show.get('permalink', '')
            }
            
            # Add derived features
            song_features = show.get('song_features', {})
            songs_played = song_features.get('songs_played', [])
            song_counts = song_features.get('song_counts', {})
            set_info = song_features.get('set_info', {})
            
            # Add summary statistics
            wide_row['total_songs'] = len(songs_played)
            wide_row['unique_songs'] = len(set(songs_played))
            wide_row['total_sets'] = len(set_info)
            wide_row['has_encore'] = 'Encore' in set_info or 'E' in set_info
            
            # Add set-specific counts
            for set_name, set_songs in set_info.items():
                wide_row[f'songs_in_{set_name.lower().replace(" ", "_")}'] = len(set_songs)
            
            # Add binary indicators for each song (1 if played, 0 if not)
            for song in self.all_songs:
                # Clean song name for column naming
                clean_song = self.clean_column_name(song)
                wide_row[f'song_{clean_song}'] = 1 if song in songs_played else 0
                
                # Add song count if played multiple times
                if song in song_counts and song_counts[song] > 1:
                    wide_row[f'song_{clean_song}_count'] = song_counts[song]
            
            wide_data.append(wide_row)
        
        # Save to CSV
        if wide_data:
            # Get all column names
            all_columns = set()
            for row in wide_data:
                all_columns.update(row.keys())
            
            # Sort columns for better readability
            base_columns = ['show_id', 'date', 'artist_name', 'venue', 'city', 'state', 'country',
                           'tour_name', 'rating', 'reviews', 'venue_id', 'permalink', 'total_songs',
                           'unique_songs', 'total_sets', 'has_encore']
            
            # Add set columns
            set_columns = [col for col in all_columns if col.startswith('songs_in_')]
            
            # Add song columns
            song_columns = sorted([col for col in all_columns if col.startswith('song_')])
            
            # Combine all columns
            fieldnames = base_columns + set_columns + song_columns
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(wide_data)
            
            logger.info(f"Wide format data saved to {filename}")
            logger.info(f"Total columns: {len(fieldnames)}")
            logger.info(f"Song columns: {len(song_columns)}")
        
        return filename
    
    def clean_column_name(self, name):
        """Clean song names for use as column names"""
        # Remove special characters and spaces, convert to lowercase
        import re
        cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        cleaned = re.sub(r'_+', '_', cleaned)  # Replace multiple underscores with single
        cleaned = cleaned.strip('_').lower()
        return cleaned
    
    def save_to_csv_ml_format(self, data, filename=None, start_year=None, end_year=None):
        """Save data in machine learning friendly format with separate song rows"""
        if not data:
            logger.error("No data to save")
            return
        
        if filename is None:
            filename = self.generate_filename(start_year, end_year, 'ml_format')
        
        logger.info(f"Converting {len(data)} shows to ML format and saving to {filename}")
        
        ml_rows = []
        
        for show in data:
            base_row = {
                'show_id': show.get('show_id', ''),
                'date': show.get('date', ''),
                'artist_name': show.get('artist_name', ''),
                'venue': show.get('venue', ''),
                'city': show.get('city', ''),
                'state': show.get('state', ''),
                'country': show.get('country', ''),
                'tour_name': show.get('tour_name', ''),
                'rating': show.get('rating', ''),
                'reviews': show.get('reviews', ''),
                'venue_id': show.get('venue_id', ''),
                'permalink': show.get('permalink', '')
            }
            
            # Parse setlist and create separate rows for each song
            setlist = show.get('setlist', '')
            if setlist:
                songs = self.parse_setlist_for_ml(setlist)
                for song_data in songs:
                    ml_row = base_row.copy()
                    ml_row.update(song_data)
                    ml_rows.append(ml_row)
            else:
                # Even if no setlist, include the show data
                ml_row = base_row.copy()
                ml_row.update({
                    'set_name': '',
                    'song_position': 0,
                    'song_name': '',
                    'has_segue': False,
                    'segue_info': ''
                })
                ml_rows.append(ml_row)
        
        # Define fieldnames for ML format
        fieldnames = [
            'show_id', 'date', 'artist_name', 'venue', 'city', 'state', 'country',
            'tour_name', 'rating', 'reviews', 'venue_id', 'permalink',
            'set_name', 'song_position', 'song_name', 'has_segue', 'segue_info'
        ]
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(ml_rows)
        
        logger.info(f"ML format data saved to {filename} with {len(ml_rows)} song rows")
        return filename
    
    def parse_setlist_for_ml(self, setlist_string):
        """Parse setlist string into individual song records for ML"""
        songs = []
        
        if not setlist_string:
            return songs
        
        # Split by sets (separated by |)
        sets = setlist_string.split('|')
        
        for set_info in sets:
            set_info = set_info.strip()
            if ':' in set_info:
                set_name, song_list = set_info.split(':', 1)
                set_name = set_name.strip()
                
                # Split songs by comma, but preserve segue info
                raw_songs = song_list.split(',')
                
                for position, raw_song in enumerate(raw_songs, 1):
                    raw_song = raw_song.strip()
                    
                    # Check for segue indicators
                    has_segue = False
                    segue_info = ''
                    song_name = raw_song
                    
                    if '>' in raw_song:
                        has_segue = True
                        segue_info = '>'
                        song_name = raw_song.replace('>', '').strip()
                    elif '->' in raw_song:
                        has_segue = True
                        segue_info = '->'
                        song_name = raw_song.replace('->', '').strip()
                    
                    if song_name:  # Only add if there's actually a song name
                        songs.append({
                            'set_name': set_name,
                            'song_position': position,
                            'song_name': song_name,
                            'has_segue': has_segue,
                            'segue_info': segue_info
                        })
        
        return songs

def main():
    """Main function to run the extraction"""
    # Initialize the extractor
    api_key = "A59D71B642DA223E6F61"  # Replace with your actual API key
    
    if not api_key or api_key == "YOUR_API_KEY_HERE":
        print("ERROR: You must provide a valid API key!")
        print("1. Go to https://api.phish.net/keys/")
        print("2. Register and get your API key")
        print("3. Replace 'YOUR_API_KEY_HERE' in the script with your actual key")
        return
    
    extractor = PhishDataExtractor(api_key=api_key)
    
    # Test the API connection first
    logger.info("Testing API connection...")
    test_response = extractor.make_request('shows/tiph.json')
    
    if test_response:
        logger.info("API connection successful!")
        if 'data' in test_response:
            logger.info(f"API is working correctly")
        else:
            logger.warning("Unexpected response format")
            logger.info(f"Response keys: {list(test_response.keys())}")
    else:
        logger.error("API connection failed. Please check your API key.")
        return
    
    # Ask user if they want to proceed with full extraction
    try:
        proceed = input("API test successful. Do you want to proceed with full extraction? (y/n): ")
        if proceed.lower() != 'y':
            logger.info("Extraction cancelled by user.")
            return
        
        # Ask for year range to limit extraction if needed
        year_range = input("Enter year range (e.g., '2020-2024') or press Enter for all years: ").strip()
        start_year = None
        end_year = None
        
        if year_range:
            try:
                if '-' in year_range:
                    start_year, end_year = map(int, year_range.split('-'))
                    logger.info(f"Extracting shows from {start_year} to {end_year}")
                else:
                    start_year = end_year = int(year_range)
                    logger.info(f"Extracting shows from {start_year} only")
            except ValueError:
                logger.warning("Invalid year range format. Using all years.")
        
    except KeyboardInterrupt:
        logger.info("Extraction cancelled by user.")
        return
    
    # Extract all data
    logger.info("Starting Phish data extraction...")
    logger.info(f"Batch size: {extractor.batch_size} shows")
    logger.info(f"Request delay: {extractor.request_delay} seconds")
    logger.info(f"Batch delay: {extractor.batch_delay} seconds")
    
    show_data = extractor.extract_all_data(start_year, end_year)
    
    # Save to CSV in multiple formats
    if show_data:
        # Save standard format
        standard_filename = extractor.save_to_csv(show_data, None, start_year, end_year, 'standard')
        
        # Save wide format for modeling
        wide_filename = extractor.save_to_csv_wide_format(show_data, None, start_year, end_year)
        
        # Save long format for ML
        ml_filename = extractor.save_to_csv_ml_format(show_data, None, start_year, end_year)
        
        logger.info(f"Extraction complete!")
        logger.info(f"Standard format saved to: {standard_filename}")
        logger.info(f"Wide format (for modeling) saved to: {wide_filename}")
        logger.info(f"Long format (for ML) saved to: {ml_filename}")
    else:
        logger.error("No data extracted")

if __name__ == "__main__":
    main()