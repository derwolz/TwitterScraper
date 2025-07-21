import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

class DatabaseManager:
    def __init__(self, db_path: str = "social_data.db"):
        self.db_path = db_path
        self.init_database()
    
    def init_database(self):
        """Initialize the database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    username TEXT PRIMARY KEY,
                    name TEXT,
                    url TEXT,
                    bio TEXT,
                    location TEXT,
                    is_verified BOOLEAN,
                    verification_type TEXT,
                    followers INTEGER,
                    following INTEGER,
                    pinned_post_link TEXT,
                    created_at TEXT,
                    is_automated BOOLEAN,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Followings relationship table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS followings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    follower_username TEXT,
                    following_username TEXT,
                    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (follower_username) REFERENCES users(username),
                    FOREIGN KEY (following_username) REFERENCES users(username),
                    UNIQUE(follower_username, following_username)
                )
            ''')
            
            conn.commit()
    
    def extract_website_link(self, profile_bio: Dict) -> Optional[str]:
        """Extract website link from profile bio entities"""
        try:
            if profile_bio and 'entities' in profile_bio:
                if 'url' in profile_bio['entities'] and 'urls' in profile_bio['entities']['url']:
                    urls = profile_bio['entities']['url']['urls']
                    if urls and len(urls) > 0:
                        return urls[0].get('expanded_url') or urls[0].get('url')
        except (KeyError, TypeError, IndexError):
            pass
        return None
    
    def extract_pinned_post_link(self, pinned_tweet_ids: List[str]) -> Optional[str]:
        """Extract first pinned post link if available"""
        if pinned_tweet_ids and len(pinned_tweet_ids) > 0:
            # Assuming Twitter URL format - you might need to adjust this
            return f"https://twitter.com/i/web/status/{pinned_tweet_ids[0]}"
        return None
    
    def store_user(self, user_data: Dict) -> bool:
        """Store or update user data"""
        try:
            website_link = self.extract_website_link(user_data.get('profile_bio'))
            pinned_post_link = self.extract_pinned_post_link(user_data.get('pinnedTweetIds', []))
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO users (
                        username, name, url, bio, location, is_verified, 
                        verification_type, followers, following, pinned_post_link,
                        created_at, is_automated, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    user_data.get('userName'),
                    user_data.get('name'),
                    website_link,
                    user_data.get('description'),
                    user_data.get('location'),
                    user_data.get('isBlueVerified', False),
                    user_data.get('verifiedType'),
                    user_data.get('followers', 0),
                    user_data.get('following', 0),
                    pinned_post_link,
                    user_data.get('createdAt'),
                    user_data.get('isAutomated', False),
                    datetime.now().isoformat()
                ))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing user {user_data.get('userName', 'Unknown')}: {e}")
            return False
    
    def store_following_relationship(self, follower_username: str, following_username: str) -> bool:
        """Store a following relationship"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR IGNORE INTO followings (follower_username, following_username)
                    VALUES (?, ?)
                ''', (follower_username, following_username))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error storing following relationship {follower_username} -> {following_username}: {e}")
            return False
    
    def store_followings_batch(self, follower_username: str, followings_data: List[Dict]) -> Tuple[int, int]:
        """Store multiple followings and their relationships"""
        stored_users = 0
        stored_relationships = 0
        
        for following_user in followings_data:
            # Store the user data
            if self.store_user(following_user):
                stored_users += 1
            
            # Store the relationship
            following_username = following_user.get('userName')
            if following_username and self.store_following_relationship(follower_username, following_username):
                stored_relationships += 1
        
        return stored_users, stored_relationships
    
    def get_user(self, username: str) -> Optional[Dict]:
        """Retrieve user data by username"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            print(f"Error retrieving user {username}: {e}")
            return None
    
    def get_user_followings(self, username: str) -> List[str]:
        """Get list of usernames that a user is following"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT following_username FROM followings 
                    WHERE follower_username = ?
                    ORDER BY scraped_at DESC
                ''', (username,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error retrieving followings for {username}: {e}")
            return []
    
    def get_user_followers(self, username: str) -> List[str]:
        """Get list of usernames that follow a user"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT follower_username FROM followings 
                    WHERE following_username = ?
                    ORDER BY scraped_at DESC
                ''', (username,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error retrieving followers for {username}: {e}")
            return []
    
    def get_stats(self) -> Dict[str, int]:
        """Get database statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                cursor.execute('SELECT COUNT(*) FROM users')
                total_users = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM followings')
                total_relationships = cursor.fetchone()[0]
                
                return {
                    'total_users': total_users,
                    'total_relationships': total_relationships
                }
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {'total_users': 0, 'total_relationships': 0}
