import requests
from typing import Dict, List, Optional, Tuple
import time
import json

class APIClient:
    def __init__(self, base_url: str = "", headers: Optional[Dict] = None):
        self.base_url = base_url
        self.headers = headers or {}
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.page_size = 200
        # Rate limiting
        self.request_delay = 1.0  # seconds between requests
        self.last_request_time = 0
    
    def _make_request(self, method: str, url: str, **kwargs) -> Optional[Dict]:
        """Make HTTP request with rate limiting"""
        # Rate limiting
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_delay:
            time.sleep(self.request_delay - time_since_last)
        
        try:
            response = self.session.request(method, url, **kwargs)
            self.last_request_time = time.time()
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Request failed with status {response.status_code}: {response.text}")
                return None
                
        except requests.RequestException as e:
            print(f"Request error: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            return None
    
    def get_user_data(self, username: str) -> Optional[Dict]:
        """
        Fetch user data for a specific username
        Replace this URL with your actual API endpoint
        """
        url = f"{self.base_url}/user/info?userName={username}"
        response = self._make_request("GET", url)
        
        if response and response.get('status') == 'success':
            return response.get('data')
        return None
    
    def get_user_followings(self, username: str, cursor: Optional[str] = None) -> Tuple[List[Dict], bool, Optional[str]]:
        """
        Fetch followings for a user with pagination
        Returns: (followings_list, has_next_page, next_cursor)
        """
        url = f"{self.base_url}/user/followings?pageSize={self.page_size}&userName={username}"
        params = {}
        if cursor:
            params['cursor'] = cursor
        
        response = self._make_request("GET", url, params=params)
        if response and response.get('status') == 'success':
            followings = response.get('followings', [])
            has_next = response.get('has_next_page', False)
            next_cursor = response.get('next_cursor')
            return followings, has_next, next_cursor
        
        return [], False, None
    
    def get_all_user_followings(self, username: str, max_pages: Optional[int] = None) -> List[Dict]:
        """
        Fetch all followings for a user, handling pagination automatically
        max_pages: optional limit on number of pages to fetch
        """
        all_followings = []
        cursor = None
        page_count = 0
        
        while True:
            if max_pages and page_count >= max_pages:
                print(f"Reached maximum pages limit ({max_pages})")
                break
            
            followings, has_next, next_cursor = self.get_user_followings(username, cursor)
            
            if not followings:
                print("No more followings found or error occurred")
                break
            
            all_followings.extend(followings)
            print(f"Retrieved {len(followings)} followings from page {page_count + 1}")
            
            if not has_next or not next_cursor:
                print("Reached end of followings")
                break
            
            cursor = next_cursor
            page_count += 1
        
        print(f"Total followings retrieved for {username}: {len(all_followings)}")
        return all_followings
    
    def set_rate_limit(self, delay_seconds: float):
        """Set the delay between requests for rate limiting"""
        self.request_delay = delay_seconds
    
    def update_headers(self, new_headers: Dict):
        """Update request headers"""
        self.headers.update(new_headers)
        self.session.headers.update(new_headers)
    
    def set_base_url(self, url: str):
        """Update the base URL"""
        self.base_url = url
    
    # Methods for handling the specific JSON structure you provided
    def parse_user_response(self, raw_response: Dict) -> Optional[Dict]:
        """Parse the user response format you provided"""
        if raw_response.get('status') == 'success' and 'data' in raw_response:
            return raw_response['data']
        return None
    
    def parse_followings_response(self, raw_response: Dict) -> Tuple[List[Dict], bool, Optional[str]]:
        """Parse the followings response format you provided"""
        if raw_response.get('status') == 'success':
            console.log("raw_response",raw_response)
            followings = raw_response.get('followings', [])
            has_next = raw_response.get('has_next_page', False)
            next_cursor = raw_response.get('next_cursor')
            return followings, has_next, next_cursor
        return [], False, None
    
    # Batch processing methods
    def process_users_batch(self, usernames: List[str]) -> List[Dict]:
        """Process multiple users in batch"""
        results = []
        for username in usernames:
            print(f"Fetching data for {username}...")
            user_data = self.get_user_data(username)
            if user_data:
                results.append(user_data)
            else:
                print(f"Failed to fetch data for {username}")
        return results
