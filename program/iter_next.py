#!/usr/bin/env python3
"""
Script to dump unprocessed users to next_gen.txt
This finds all users in the database who haven't had their followings scraped yet
and writes them to a text file for the next generation of processing.
"""

from database_manager import DatabaseManager
from config import Config
import os

def dump_unprocessed_users():
    """Dump unprocessed users to next_gen.txt"""
    
    print("Connecting to database...")
    db_manager = DatabaseManager(Config.DATABASE_PATH)
    
    # Get unprocessed users
    print("Finding unprocessed users...")
    unprocessed_users = db_manager.get_unprocessed_users()
    
    if not unprocessed_users:
        print("No unprocessed users found!")
        print("All users in the database have already been processed.")
        return
    
    # Sort users alphabetically for consistency
    unprocessed_users.sort()
    
    # Write to next_gen.txt
    output_file = "next_gen.txt"
    print(f"Writing {len(unprocessed_users)} unprocessed users to {output_file}...")
    
    try:
        with open(output_file, 'w') as f:
            for username in unprocessed_users:
                f.write(f"{username}\n")
        
        print(f"Successfully wrote {len(unprocessed_users)} users to {output_file}")
        
        # Show some stats
        db_stats = db_manager.get_stats()
        print(f"\nDatabase summary:")
        print(f"  - Total users in database: {db_stats['total_users']}")
        print(f"  - Processed users: {db_stats['processed_users']}")
        print(f"  - Unprocessed users: {db_stats['unprocessed_users']}")
        print(f"  - Failed users: {db_stats['failed_users']}")
        print(f"  - Users written to {output_file}: {len(unprocessed_users)}")
        
        # Show first few users as preview
        if unprocessed_users:
            print(f"\nFirst few users in {output_file}:")
            for i, username in enumerate(unprocessed_users[:5]):
                print(f"  {i+1}. {username}")
            if len(unprocessed_users) > 5:
                print(f"  ... and {len(unprocessed_users) - 5} more")
        
    except Exception as e:
        print(f"Error writing to {output_file}: {e}")
        return
    
    print(f"\nNext steps:")
    print(f"1. Review {output_file} to see the users that need processing")
    print(f"2. Use {output_file} as input for your main scraping script")
    print(f"3. These users will become the 'next generation' to process")

def show_processing_breakdown():
    """Show detailed breakdown of user processing status"""
    db_manager = DatabaseManager(Config.DATABASE_PATH)
    
    print("\n" + "="*50)
    print("DETAILED PROCESSING BREAKDOWN")
    print("="*50)
    
    # Get all status categories
    unprocessed = db_manager.get_unprocessed_users()
    processed = db_manager.get_processed_users()
    failed = db_manager.get_failed_users()
    
    print(f"\n📊 SUMMARY:")
    print(f"  ✅ Successfully processed: {len(processed)}")
    print(f"  ⏳ Awaiting processing: {len(unprocessed)}")
    print(f"  ❌ Failed processing: {len(failed)}")
    print(f"  📁 Total users in database: {len(processed) + len(unprocessed) + len(failed)}")
    
    if processed:
        print(f"\n✅ SUCCESSFULLY PROCESSED USERS ({len(processed)}):")
        for username in processed[:10]:  # Show first 10
            status = db_manager.get_processing_status(username)
            count = status.get('followings_count', 0) if status else 0
            print(f"  - {username} ({count} followings)")
        if len(processed) > 10:
            print(f"  ... and {len(processed) - 10} more")
    
    if failed:
        print(f"\n❌ FAILED USERS ({len(failed)}):")
        for user_info in failed[:5]:  # Show first 5 failures
            print(f"  - {user_info['username']}: {user_info['error_message']}")
        if len(failed) > 5:
            print(f"  ... and {len(failed) - 5} more failures")
    
    if unprocessed:
        print(f"\n⏳ UNPROCESSED USERS ({len(unprocessed)}) → Going to next_gen.txt:")
        for username in unprocessed[:10]:  # Show first 10
            print(f"  - {username}")
        if len(unprocessed) > 10:
            print(f"  ... and {len(unprocessed) - 10} more")

if __name__ == "__main__":
    print("=" * 60)
    print("NEXT GENERATION USER DUMP SCRIPT")
    print("=" * 60)
    print("This script finds users who haven't been processed yet")
    print("and dumps them to next_gen.txt for the next scraping run.")
    print()
    
    try:
        # Check if database exists
        if not os.path.exists(Config.DATABASE_PATH):
            print(f"❌ Database not found at {Config.DATABASE_PATH}")
            print("Run the main scraping script first to create the database.")
            exit(1)
        
        # Show detailed breakdown
        show_processing_breakdown()
        
        # Dump unprocessed users
        print("\n" + "="*50)
        print("DUMPING UNPROCESSED USERS")
        print("="*50)
        dump_unprocessed_users()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        exit(1)
    
    print("\n✅ Script completed successfully!")
