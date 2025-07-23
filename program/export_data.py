#!/usr/bin/env python3
"""
Export script to create folder structure and stats CSV from the social data database.

Creates:
- twitterdata/following/ (with username.txt files containing following lists)
- twitterdata/followers/ (with username.txt files containing follower lists)  
- twitterdata/stats.csv (summary statistics)
"""

import os
import csv
import sqlite3
from typing import List, Dict, Tuple
from database_manager import DatabaseManager
from config import Config

class DataExporter:
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Config.DATABASE_PATH
        self.output_dir = "twitterdata"
        self.following_dir = os.path.join(self.output_dir, "following")
        self.followers_dir = os.path.join(self.output_dir, "followers")
        
    def create_directories(self):
        """Create the required directory structure"""
        os.makedirs(self.following_dir, exist_ok=True)
        os.makedirs(self.followers_dir, exist_ok=True)
        print(f"Created directory structure: {self.output_dir}/")
        
    def load_users_from_file(self, filename: str = "users.txt") -> List[str]:
        """Load usernames from the users.txt file"""
        try:
            with open(filename, 'r') as f:
                usernames = [line.strip() for line in f if line.strip()]
            print(f"Loaded {len(usernames)} usernames from {filename}")
            return usernames
        except FileNotFoundError:
            print(f"File {filename} not found.")
            return []
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            return []
    
    def get_user_following_from_db(self, username: str) -> List[str]:
        """Get list of usernames that a user is following from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT following_username FROM followings 
                    WHERE follower_username = ?
                    ORDER BY following_username
                ''', (username,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error retrieving following for {username}: {e}")
            return []
    
    def get_user_followers_from_db(self, username: str) -> List[str]:
        """Get list of usernames that follow a user from database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT follower_username FROM followings 
                    WHERE following_username = ?
                    ORDER BY follower_username
                ''', (username,))
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Error retrieving followers for {username}: {e}")
            return []
    
    def get_user_stats_from_db(self, username: str) -> Dict:
        """Get user stats from the users table"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT followers, following 
                    FROM users 
                    WHERE username = ?
                ''', (username,))
                row = cursor.fetchone()
                if row:
                    return {
                        'reported_followers': row['followers'] or 0,
                        'reported_following': row['following'] or 0
                    }
                return {'reported_followers': 0, 'reported_following': 0}
        except Exception as e:
            print(f"Error retrieving stats for {username}: {e}")
            return {'reported_followers': 0, 'reported_following': 0}
    
    def export_user_lists(self, username: str) -> Tuple[int, int]:
        """Export following and followers lists for a user"""
        # Export following list
        following_list = self.get_user_following_from_db(username)
        following_file = os.path.join(self.following_dir, f"{username}.txt")
        
        try:
            with open(following_file, 'w') as f:
                for following_user in following_list:
                    f.write(f"{following_user}\n")
            print(f"Exported {len(following_list)} following for {username}")
        except Exception as e:
            print(f"Error writing following file for {username}: {e}")
        
        # Export followers list
        followers_list = self.get_user_followers_from_db(username)
        followers_file = os.path.join(self.followers_dir, f"{username}.txt")
        
        try:
            with open(followers_file, 'w') as f:
                for follower_user in followers_list:
                    f.write(f"{follower_user}\n")
            print(f"Exported {len(followers_list)} followers for {username}")
        except Exception as e:
            print(f"Error writing followers file for {username}: {e}")
        
        return len(following_list), len(followers_list)
    
    def create_stats_csv(self, usernames: List[str]):
        """Create stats.csv with summary information"""
        stats_file = os.path.join(self.output_dir, "stats.csv")
        
        with open(stats_file, 'w', newline='') as csvfile:
            fieldnames = [
                'username',
                'reported_following',
                'reported_followers', 
                'processed_following',
                'processed_followers'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for username in usernames:
                # Get reported stats from user profile
                user_stats = self.get_user_stats_from_db(username)
                
                # Get processed counts from our database
                processed_following = len(self.get_user_following_from_db(username))
                processed_followers = len(self.get_user_followers_from_db(username))
                
                writer.writerow({
                    'username': username,
                    'reported_following': user_stats['reported_following'],
                    'reported_followers': user_stats['reported_followers'],
                    'processed_following': processed_following,
                    'processed_followers': processed_followers
                })
                
                print(f"Stats for {username}: "
                      f"Following {processed_following}/{user_stats['reported_following']}, "
                      f"Followers {processed_followers}/{user_stats['reported_followers']}")
        
        print(f"Stats CSV created: {stats_file}")
    
    def export_all_data(self):
        """Main method to export all data"""
        print("Starting data export...")
        
        # Create directory structure
        self.create_directories()
        
        # Load users from file
        usernames = self.load_users_from_file()
        
        if not usernames:
            print("No users found in users.txt. Exiting.")
            return
        
        print(f"Exporting data for {len(usernames)} users...")
        
        # Export individual user lists
        total_following_exported = 0
        total_followers_exported = 0
        
        for username in usernames:
            print(f"\nProcessing {username}...")
            following_count, followers_count = self.export_user_lists(username)
            total_following_exported += following_count
            total_followers_exported += followers_count
        
        # Create stats CSV
        print(f"\nCreating stats CSV...")
        self.create_stats_csv(usernames)
        
        print(f"\nExport complete!")
        print(f"Total following exported: {total_following_exported}")
        print(f"Total followers exported: {total_followers_exported}")
        print(f"Files created in: {os.path.abspath(self.output_dir)}")

def main():
    """Main function to run the export"""
    exporter = DataExporter()
    exporter.export_all_data()

if __name__ == "__main__":
    main()
