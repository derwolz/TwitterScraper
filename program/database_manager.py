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
                    media_count INTEGER,
                    status_count INTEGER,
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

           # AI analysis table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ai_analysis (
                    username TEXT PRIMARY KEY,
                    ai_bio BOOLEAN NOT NULL,
                    found_keywords TEXT,
                    keyword_count INTEGER DEFAULT 0,
                    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (username) REFERENCES users(username)
                )
            ''')

            # User processing status table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS user_processing_status (
                    username TEXT PRIMARY KEY,
                    followings_scraped BOOLEAN DEFAULT FALSE,
                    followings_scraped_at TIMESTAMP,
                    followings_count INTEGER DEFAULT 0,
                    pages_scraped INTEGER DEFAULT 0,
                    max_pages_attempted INTEGER DEFAULT 0,
                    is_complete BOOLEAN DEFAULT FALSE,
                    error_message TEXT,
                    last_attempt_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (username) REFERENCES users(username)
                )
            ''')
            
            conn.commit()
    
    def user_exists(self, username: str) -> bool:
        """Check if a user exists in the database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('SELECT 1 FROM users WHERE username = ? LIMIT 1', (username.lower(),))
                return cursor.fetchone() is not None
        except Exception as e:
            print(f"Error checking if user {username} exists: {e}")
            return False
    
    def users_exist_batch(self, usernames: List[str]) -> Dict[str, bool]:
        """Check which users exist in the database in batch"""
        try:
            # Normalize usernames to lowercase
            normalized_usernames = [username.lower() for username in usernames]
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Create a placeholder string for the IN query
                placeholders = ','.join(['?' for _ in normalized_usernames])
                cursor.execute(f'SELECT username FROM users WHERE username IN ({placeholders})', normalized_usernames)
                existing_users = {row[0] for row in cursor.fetchall()}
                
                # Return dict with existence status for each original username
                return {username: username.lower() in existing_users for username in usernames}
        except Exception as e:
            print(f"Error checking batch user existence: {e}")
            return {username: False for username in usernames}
    
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
            
            # Normalize username to lowercase
            username = user_data.get('userName', '').lower()
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO users (
                        username, name, url, bio, location, is_verified, 
                        verification_type, followers, following, pinned_post_link,
                        media_count,status_count, created_at, is_automated, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    username,
                    user_data.get('name'),
                    website_link,
                    user_data.get('description'),
                    user_data.get('location'),
                    user_data.get('isBlueVerified', False),
                    user_data.get('verifiedType'),
                    user_data.get('followers', 0),
                    user_data.get('following', 0),
                    pinned_post_link,
                    user_data.get('mediaCount',0),
                    user_data.get('statusesCount',0),
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
                ''', (follower_username.lower(), following_username.lower()))
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error storing following relationship {follower_username} -> {following_username}: {e}")
            return False
    
    def store_followings_batch_with_check(self, follower_username: str, followings_data: List[Dict]) -> Tuple[int, int, int]:
        """
        Store multiple followings and their relationships, checking for existing users first
        Returns: (new_users_stored, existing_users_found, relationships_stored)
        """
        if not followings_data:
            return 0, 0, 0
        
        # Extract usernames from the followings data
        following_usernames = [user.get('userName') for user in followings_data if user.get('userName')]
        
        # Check which users already exist in batch
        user_existence = self.users_exist_batch(following_usernames)
        
        new_users_stored = 0
        existing_users_found = 0
        relationships_stored = 0
        
        for following_user in followings_data:
            following_username = following_user.get('userName')
            if not following_username:
                continue
            
            # Normalize username to lowercase
            following_username = following_username.lower()
            
            # Check if user already exists
            if user_existence.get(following_user.get('userName'), False):
                print(f"User {following_username} already exists in database, skipping user storage")
                existing_users_found += 1
            else:
                # Store new user data
                if self.store_user(following_user):
                    print(f"Stored new user: {following_username}")
                    new_users_stored += 1
                else:
                    print(f"Failed to store new user: {following_username}")
            
            # Store the relationship regardless of whether user was new or existing
            if self.store_following_relationship(follower_username, following_username):
                relationships_stored += 1
        
        print(f"Batch processing complete: {new_users_stored} new users, {existing_users_found} existing users, {relationships_stored} relationships")
        return new_users_stored, existing_users_found, relationships_stored
    
    def store_followings_batch(self, follower_username: str, followings_data: List[Dict]) -> Tuple[int, int]:
        """Store multiple followings and their relationships (original method for compatibility)"""
        new_users, existing_users, relationships = self.store_followings_batch_with_check(follower_username, followings_data)
        return new_users, relationships
    
    def get_user(self, username: str) -> Optional[Dict]:
        """Retrieve user data by username"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM users WHERE username = ?', (username.lower(),))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            print(f"Error retrieving user {username}: {e}")
            return None
    def get_all_users(self) -> List[Dict]:
        """Retrieve all users from the database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM users ORDER BY username')
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error retrieving all users: {e}")
            return []
    def get_user_followings(self, username: str) -> List[str]:
        """Get list of usernames that a user is following"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT following_username FROM followings 

                    WHERE follower_username = ?
                    ORDER BY scraped_at DESC
                ''', (username.lower(),))
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
                ''', (username.lower(),))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error retrieving followers for {username}: {e}")
            return []
    
    def mark_followings_scraped(self, username: str, followings_count: int, pages_scraped: int, max_pages: int, is_complete: bool = True, error_message: Optional[str] = None) -> bool:
        """Mark that a user's followings have been scraped"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO user_processing_status (
                        username, followings_scraped, followings_scraped_at, 
                        followings_count, pages_scraped, max_pages_attempted,
                        is_complete, error_message, last_attempt_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    username.lower(), True, datetime.now().isoformat(),
                    followings_count, pages_scraped, max_pages,
                    is_complete, error_message, datetime.now().isoformat()
                ))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error marking followings scraped for {username}: {e}")
            return False
    
    def is_followings_scraped(self, username: str) -> bool:
        """Check if a user's followings have been scraped"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT followings_scraped FROM user_processing_status 
                    WHERE username = ? AND followings_scraped = 1
                ''', (username.lower(),))
                result = cursor.fetchone()
                return result is not None
        except Exception as e:
            print(f"Error checking if followings scraped for {username}: {e}")
            return False
    
    def get_processing_status(self, username: str) -> Optional[Dict]:
        """Get detailed processing status for a user"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT * FROM user_processing_status WHERE username = ?
                ''', (username.lower(),))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception as e:
            print(f"Error getting processing status for {username}: {e}")
            return None
    
    def get_unprocessed_users(self) -> List[str]:
        """Get list of users who haven't had their followings scraped yet"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT u.username FROM users u
                    LEFT JOIN user_processing_status ups ON u.username = ups.username
                    WHERE ups.followings_scraped IS NULL OR ups.followings_scraped = 0
                ''')
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting unprocessed users: {e}")
            return []
    
    def get_processed_users(self) -> List[str]:
        """Get list of users who have had their followings scraped"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT username FROM user_processing_status 
                    WHERE followings_scraped = 1 AND is_complete = 1
                ''')
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting processed users: {e}")
            return []
    
    def get_failed_users(self) -> List[Dict]:
        """Get list of users whose following scraping failed"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT username, error_message, last_attempt_at 
                    FROM user_processing_status 
                    WHERE is_complete = 0 AND error_message IS NOT NULL
                ''')
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error getting failed users: {e}")
            return []
    
    def reset_processing_status(self, username: str) -> bool:
        """Reset processing status for a user (to reprocess them)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    DELETE FROM user_processing_status WHERE username = ?
                ''', (username.lower(),))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error resetting processing status for {username}: {e}")
            return False
    
    def store_ai_analysis(self, username: str, ai_bio: bool, found_keywords: List[str]) -> bool:
        """Store AI analysis results for a user"""
        try:
            keywords_json = json.dumps(found_keywords) if found_keywords else '[]'
            
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO ai_analysis (
                        username, ai_bio, found_keywords, keyword_count, updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                ''', (
                    username, 
                    ai_bio, 
                    keywords_json, 
                    len(found_keywords), 
                    datetime.now().isoformat()
                ))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing AI analysis for {username}: {e}")
            return False
    
    def get_ai_analysis(self, username: str) -> Optional[Dict]:
        """Get AI analysis results for a specific user"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('SELECT * FROM ai_analysis WHERE username = ?', (username,))
                row = cursor.fetchone()
                if row:
                    result = dict(row)
                    # Parse the JSON keywords back to list
                    result['found_keywords'] = json.loads(result['found_keywords']) if result['found_keywords'] else []
                    return result
                return None
        except Exception as e:
            print(f"Error retrieving AI analysis for {username}: {e}")
            return None
    
    def get_ai_users(self) -> List[Dict]:
        """Get all users marked as AI-related with their details"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT u.username, u.name, u.bio, u.followers, u.following, 
                           a.found_keywords, a.keyword_count, a.analyzed_at
                    FROM users u
                    JOIN ai_analysis a ON u.username = a.username
                    WHERE a.ai_bio = 1
                    ORDER BY a.keyword_count DESC, u.followers DESC
                ''')
                rows = cursor.fetchall()
                results = []
                for row in rows:
                    result = dict(row)
                    result['found_keywords'] = json.loads(result['found_keywords']) if result['found_keywords'] else []
                    results.append(result)
                return results
        except Exception as e:
            print(f"Error retrieving AI users: {e}")
            return []
    
    def get_non_ai_users(self) -> List[Dict]:
        """Get all users marked as non-AI-related"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT u.username, u.name, u.bio, u.followers, u.following, a.analyzed_at
                    FROM users u
                    JOIN ai_analysis a ON u.username = a.username
                    WHERE a.ai_bio = 0
                    ORDER BY u.followers DESC
                ''')
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error retrieving non-AI users: {e}")
            return []
    
    def get_unanalyzed_users(self) -> List[Dict]:
        """Get users who haven't been analyzed for AI keywords yet"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT u.username, u.name, u.bio, u.followers, u.following
                    FROM users u
                    LEFT JOIN ai_analysis a ON u.username = a.username
                    WHERE a.username IS NULL
                    ORDER BY u.followers DESC
                ''')
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
        except Exception as e:
            print(f"Error retrieving unanalyzed users: {e}")
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
                
                cursor.execute('SELECT COUNT(*) FROM user_processing_status WHERE followings_scraped = 1')
                processed_users = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM user_processing_status WHERE is_complete = 0')
                failed_users = cursor.fetchone()[0]
                
                unprocessed_count = len(self.get_unprocessed_users())
                
                return {
                    'total_users': total_users,
                    'total_relationships': total_relationships,
                    'processed_users': processed_users,
                    'unprocessed_users': unprocessed_count,
                    'failed_users': failed_users
                }
        except Exception as e:
            print(f"Error getting stats: {e}")
            return {'total_users': 0, 'total_relationships': 0, 'processed_users': 0, 'unprocessed_users': 0, 'failed_users': 0}

    def get_ai_stats(self) -> Dict[str, any]:
        """Get detailed AI analysis statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Basic counts
                cursor.execute('SELECT COUNT(*) FROM ai_analysis WHERE ai_bio = 1')
                ai_count = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM ai_analysis WHERE ai_bio = 0')
                non_ai_count = cursor.fetchone()[0]
                
                total_analyzed = ai_count + non_ai_count
                
                # Most common keywords
                cursor.execute('''
                    SELECT found_keywords FROM ai_analysis 
                    WHERE ai_bio = 1 AND found_keywords != '[]'
                ''')
                all_keywords = []
                for row in cursor.fetchall():
                    keywords = json.loads(row[0])
                    all_keywords.extend(keywords)
                
                # Count keyword frequency
                keyword_counts = {}
                for keyword in all_keywords:
                    keyword_counts[keyword] = keyword_counts.get(keyword, 0) + 1
                
                # Top keywords
                top_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:10]
                
                return {
                    'total_analyzed': total_analyzed,
                    'ai_users': ai_count,
                    'non_ai_users': non_ai_count,
                    'ai_percentage': round((ai_count / total_analyzed * 100) if total_analyzed > 0 else 0, 2),
                    'total_keywords_found': len(all_keywords),
                    'unique_keywords': len(keyword_counts),
                    'top_keywords': top_keywords
                }
                
        except Exception as e:
            print(f"Error getting AI stats: {e}")
            return {}
