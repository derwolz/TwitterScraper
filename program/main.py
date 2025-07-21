from database_manager import DatabaseManager
from api_client import APIClient
from config import Config
from typing import List, Dict, Optional

class SocialDataCollector:
    def __init__(self):
        self.api_client = APIClient(Config.API_BASE_URL, Config.API_HEADERS)
        self.api_client.set_rate_limit(Config.REQUEST_DELAY)
        self.db_manager = DatabaseManager(Config.DATABASE_PATH)
    
    def collect_user_and_followings(self, username: str, max_pages: Optional[int] = None) -> Dict[str, int]:
        if max_pages is None:
            max_pages = Config.MAX_PAGES_PER_USER
        """
        Main method to collect user data and all their followings
        Returns statistics about what was collected
        """
        print(f"Starting collection for user: {username}")
        
        # First, get the user's own data
        user_data = self.api_client.get_user_data(username)
        if not user_data:
            print(f"Failed to retrieve user data for {username}")
            return {"error": "Failed to retrieve user data"}
        
        # Store the main user
        if self.db_manager.store_user(user_data):
            print(f"Stored user data for {username}")
        else:
            print(f"Failed to store user data for {username}")
        
        # Get all followings
        followings_data = self.api_client.get_all_user_followings(username, max_pages)
        
        if not followings_data:
            print(f"No followings found for {username}")
            return {"main_user": 1, "followings": 0, "relationships": 0}
        
        # Store followings and relationships
        stored_users, stored_relationships = self.db_manager.store_followings_batch(username, followings_data)
        
        stats = {
            "main_user": 1,
            "followings": stored_users,
            "relationships": stored_relationships,
            "total_followings_fetched": len(followings_data)
        }
        
        print(f"Collection complete for {username}:")
        print(f"  - Main user stored: {stats['main_user']}")
        print(f"  - Following users stored: {stats['followings']}")
        print(f"  - Relationships stored: {stats['relationships']}")
        print(f"  - Total followings fetched: {stats['total_followings_fetched']}")
        
        return stats
    
    def collect_multiple_users(self, usernames: List[str], max_pages_per_user: Optional[int] = None) -> Dict[str, Dict]:
        """
        Collect data for multiple users
        """
        results = {}
        
        for username in usernames:
            try:
                result = self.collect_user_and_followings(username, max_pages_per_user)
                results[username] = result
            except Exception as e:
                print(f"Error collecting data for {username}: {e}")
                results[username] = {"error": str(e)}
        
        return results
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get current database statistics"""
        return self.db_manager.get_stats()
    
    def configure_api(self, base_url: str = "", headers: Dict = None, rate_limit: float = 1.0):
        """Configure API client settings"""
        if base_url:
            self.api_client.set_base_url(base_url)
        if headers:
            self.api_client.update_headers(headers)
        self.api_client.set_rate_limit(rate_limit)

# Example usage
def load_users_from_file(filename: str = "users.txt") -> List[str]:
    """Load usernames from a text file, one per line"""
    try:
        with open(filename, 'r') as f:
            usernames = [line.strip() for line in f if line.strip()]
        print(f"Loaded {len(usernames)} usernames from {filename}")
        return usernames
    except FileNotFoundError:
        print(f"File {filename} not found. Please create it with one username per line.")
        return []
    except Exception as e:
        print(f"Error reading {filename}: {e}")
        return []

if __name__ == "__main__":
    collector = SocialDataCollector()
    
    # Load users from file
    usernames = load_users_from_file()
    
    if usernames:
        print(f"Starting collection for {len(usernames)} users...")
        results = collector.collect_multiple_users(usernames)
        
        # Print summary
        total_success = sum(1 for r in results.values() if "error" not in r)
        print(f"\nCollection complete: {total_success}/{len(usernames)} users successful")
        
        # Show database stats
        db_stats = collector.get_database_stats()
        print(f"Database stats: {db_stats}")
    else:
        print("No users to process. Add usernames to users.txt")
