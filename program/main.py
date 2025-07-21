from database_manager import DatabaseManager
from api_client import APIClient
from config import Config
from typing import List, Dict, Optional

class SocialDataCollector:
    def __init__(self):
        self.api_client = APIClient(Config.API_BASE_URL, Config.API_HEADERS)
        self.api_client.set_rate_limit(Config.REQUEST_DELAY)
        self.db_manager = DatabaseManager(Config.DATABASE_PATH)
    
    def collect_user_and_followings(self, username: str, max_pages: Optional[int] = None, force_reprocess: bool = False) -> Dict[str, int]:
        if max_pages is None:
            max_pages = Config.MAX_PAGES_PER_USER
        """
        Main method to collect user data and all their followings
        Returns statistics about what was collected
        """
        print(f"Starting collection for user: {username}")
        
        # Check if user has already been processed
        if not force_reprocess and self.db_manager.is_followings_scraped(username):
            print(f"User {username} has already been processed. Use force_reprocess=True to reprocess.")
            processing_status = self.db_manager.get_processing_status(username)
            return {
                "already_processed": True,
                "followings_count": processing_status.get('followings_count', 0) if processing_status else 0,
                "message": "User already processed"
            }
        
        # First, get the user's own data
        user_data = self.api_client.get_user_data(username)
        if not user_data:
            error_msg = f"Failed to retrieve user data for {username}"
            print(error_msg)
            # Mark as failed
            self.db_manager.mark_followings_scraped(username, 0, 0, max_pages, False, error_msg)
            return {"error": "Failed to retrieve user data"}
        
        # Store the main user
        if self.db_manager.store_user(user_data):
            print(f"Stored user data for {username}")
        else:
            print(f"Failed to store user data for {username}")
        
        # Get all followings
        try:
            followings_data = self.api_client.get_all_user_followings(username, max_pages)
            
            if not followings_data:
                print(f"No followings found for {username}")
                # Mark as complete even with 0 followings
                self.db_manager.mark_followings_scraped(username, 0, 0, max_pages, True)
                return {"main_user": 1, "followings": 0, "relationships": 0}
            
            # Store followings and relationships with existence check
            new_users, existing_users, stored_relationships = self.db_manager.store_followings_batch_with_check(username, followings_data)
            
            # Calculate pages scraped (estimate based on page size)
            pages_scraped = (len(followings_data) + self.api_client.page_size - 1) // self.api_client.page_size
            
            # Mark as successfully processed
            self.db_manager.mark_followings_scraped(username, len(followings_data), pages_scraped, max_pages, True)
            
            stats = {
                "main_user": 1,
                "new_followings": new_users,
                "existing_followings": existing_users,
                "total_followings": new_users + existing_users,
                "relationships": stored_relationships,
                "total_followings_fetched": len(followings_data),
                "pages_scraped": pages_scraped
            }
            
            print(f"Collection complete for {username}:")
            print(f"  - Main user stored: {stats['main_user']}")
            print(f"  - New following users stored: {stats['new_followings']}")
            print(f"  - Existing following users found: {stats['existing_followings']}")
            print(f"  - Total following users processed: {stats['total_followings']}")
            print(f"  - Relationships stored: {stats['relationships']}")
            print(f"  - Total followings fetched: {stats['total_followings_fetched']}")
            print(f"  - Pages scraped: {stats['pages_scraped']}")
            
            return stats
            
        except Exception as e:
            error_msg = f"Error during followings collection: {str(e)}"
            print(error_msg)
            # Mark as failed
            self.db_manager.mark_followings_scraped(username, 0, 0, max_pages, False, error_msg)
            return {"error": error_msg}
    
    def collect_multiple_users(self, usernames: List[str], max_pages_per_user: Optional[int] = None, force_reprocess: bool = False) -> Dict[str, Dict]:
        """
        Collect data for multiple users
        """
        results = {}
        
        for username in usernames:
            try:
                result = self.collect_user_and_followings(username, max_pages_per_user, force_reprocess)
                results[username] = result
            except Exception as e:
                print(f"Error collecting data for {username}: {e}")
                results[username] = {"error": str(e)}
                # Mark as failed in database
                self.db_manager.mark_followings_scraped(username, 0, 0, max_pages_per_user or Config.MAX_PAGES_PER_USER, False, str(e))
        
        return results
    
    def get_unprocessed_users(self) -> List[str]:
        """Get list of users who haven't been processed yet"""
        return self.db_manager.get_unprocessed_users()
    
    def get_processed_users(self) -> List[str]:
        """Get list of users who have been processed"""
        return self.db_manager.get_processed_users()
    
    def get_failed_users(self) -> List[Dict]:
        """Get list of users whose processing failed"""
        return self.db_manager.get_failed_users()
    
    def get_processing_status(self, username: str) -> Optional[Dict]:
        """Get processing status for a specific user"""
        return self.db_manager.get_processing_status(username)
    
    def reset_user_processing(self, username: str) -> bool:
        """Reset processing status for a user to reprocess them"""
        return self.db_manager.reset_processing_status(username)
    
    def check_user_exists(self, username: str) -> bool:
        """Check if a user exists in the database"""
        return self.db_manager.user_exists(username)
    
    def check_users_exist(self, usernames: List[str]) -> Dict[str, bool]:
        """Check which users exist in the database"""
        return self.db_manager.users_exist_batch(usernames)
    
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
        
        # Check processing status
        unprocessed_users = []
        already_processed = []
        
        for username in usernames:
            if collector.db_manager.is_followings_scraped(username):
                already_processed.append(username)
            else:
                unprocessed_users.append(username)
        
        if already_processed:
            print(f"Found {len(already_processed)} already processed users: {already_processed}")
            print("Use force_reprocess=True in collect_user_and_followings() to reprocess them.")
        
        if unprocessed_users:
            print(f"Found {len(unprocessed_users)} unprocessed users: {unprocessed_users}")
            
            # Collect data for unprocessed users only
            results = collector.collect_multiple_users(unprocessed_users)
            
            # Print summary for new collections
            total_success = sum(1 for r in results.values() if "error" not in r and not r.get("already_processed", False))
            total_new_followings = sum(r.get('new_followings', 0) for r in results.values() if "error" not in r)
            total_existing_followings = sum(r.get('existing_followings', 0) for r in results.values() if "error" not in r)
            total_relationships = sum(r.get('relationships', 0) for r in results.values() if "error" not in r)
            
            print(f"\nNew collection complete: {total_success}/{len(unprocessed_users)} users successful")
            print(f"Total new followings stored: {total_new_followings}")
            print(f"Total existing followings found: {total_existing_followings}")
            print(f"Total relationships created: {total_relationships}")
        else:
            print("All users have already been processed!")
        
        # Show comprehensive database stats
        db_stats = collector.get_database_stats()
        print(f"\nDatabase stats:")
        print(f"  - Total users: {db_stats['total_users']}")
        print(f"  - Total relationships: {db_stats['total_relationships']}")
        print(f"  - Processed users: {db_stats['processed_users']}")
        print(f"  - Unprocessed users: {db_stats['unprocessed_users']}")
        print(f"  - Failed users: {db_stats['failed_users']}")
        
        # Show failed users if any
        failed_users = collector.get_failed_users()
        if failed_users:
            print(f"\nFailed users ({len(failed_users)}):")
            for failed in failed_users:
                print(f"  - {failed['username']}: {failed['error_message']} (at {failed['last_attempt_at']})")
    else:
        print("No users to process. Add usernames to users.txt")
