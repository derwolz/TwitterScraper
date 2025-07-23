#!/usr/bin/env python3
"""
Script to analyze user bios for AI-related keywords.
This script can be run independently to process all users in the database.
"""

import sys
import os
from typing import Dict, List

# Add the current directory to path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ai_keyword_detector import AIKeywordDetector
from database_manager import DatabaseManager
from config import Config

def print_banner():
    """Print a nice banner for the script"""
    print("=" * 60)
    print("🤖 AI Bio Keyword Analysis Tool")
    print("=" * 60)
    print()

def print_summary_stats(stats: Dict):
    """Print a formatted summary of the analysis"""
    print("\n" + "=" * 60)
    print("📊 ANALYSIS SUMMARY")
    print("=" * 60)
    print(f"Total users analyzed: {stats['total_analyzed']}")
    print(f"AI-related users: {stats['ai_related_users']} ({stats['ai_percentage']}%)")
    print(f"Non-AI users: {stats['total_analyzed'] - stats['ai_related_users']}")
    print()

def print_sample_ai_users(ai_users: List[Dict], limit: int = 10):
    """Print sample AI users found"""
    if not ai_users:
        print("No AI-related users found.")
        return
    
    print(f"🎯 SAMPLE AI-RELATED USERS (showing top {min(limit, len(ai_users))}):")
    print("-" * 60)
    
    for i, user in enumerate(ai_users[:limit], 1):
        print(f"{i}. @{user['username']}")
        if user.get('name'):
            print(f"   Name: {user['name']}")
        print(f"   Bio: {user['bio'][:100]}{'...' if len(user['bio']) > 100 else ''}")
        print(f"   Keywords: {', '.join(user['found_keywords'][:5])}{'...' if len(user['found_keywords']) > 5 else ''}")
        print(f"   Followers: {user.get('followers', 'N/A')}")
        print()

def print_top_keywords(detector: AIKeywordDetector):
    """Print the most common AI keywords found"""
    db_stats = detector.db_manager.get_ai_stats()
    
    if db_stats.get('top_keywords'):
        print("🔥 MOST COMMON AI KEYWORDS:")
        print("-" * 30)
        for keyword, count in db_stats['top_keywords'][:15]:
            print(f"{keyword:<25} {count:>3} times")
        print()

def main():
    """Main function to run the AI bio analysis"""
    print_banner()
    
    # Initialize the detector
    detector = AIKeywordDetector()
    
    # Check if there are users to analyze
    db_stats = detector.db_manager.get_stats()
    print(f"Database contains {db_stats['total_users']} users")
    print(f"Already analyzed: {db_stats.get('analyzed_users', 0)} users")
    print(f"Unanalyzed: {db_stats.get('unanalyzed_users', db_stats['total_users'])} users")
    print()
    
    if db_stats['total_users'] == 0:
        print("❌ No users found in database. Please run the data collection first.")
        return
    
    # Ask user what they want to do
    print("What would you like to do?")
    print("1. Analyze all users (including re-analysis)")
    print("2. Analyze only unanalyzed users")
    print("3. Show existing results")
    print("4. Show detailed statistics")
    print("5. Test keyword detection on sample bios")
    
    choice = input("\nEnter your choice (1-5): ").strip()
    
    if choice == "1":
        # Analyze all users
        print("\n🔄 Starting analysis of ALL users...")
        results = detector.analyze_all_users()
        print_summary_stats(results)
        
        # Show sample AI users
        ai_users = detector.get_ai_users()
        print_sample_ai_users(ai_users)
        print_top_keywords(detector)
        
    elif choice == "2":
        # Analyze only unanalyzed users
        unanalyzed_users = detector.db_manager.get_unanalyzed_users()
        
        if not unanalyzed_users:
            print("\n✅ All users have already been analyzed!")
            choice = "3"  # Fall through to show results
        else:
            print(f"\n🔄 Analyzing {len(unanalyzed_users)} unanalyzed users...")
            
            ai_count = 0
            for i, user in enumerate(unanalyzed_users, 1):
                username = user['username']
                print(f"Analyzing {i}/{len(unanalyzed_users)}: {username}")
                
                result = detector.analyze_user_bio(username)
                if not result.get('error') and result.get('is_ai_related'):
                    ai_count += 1
                    print(f"  ✓ AI-related! Keywords: {result['found_keywords'][:3]}...")
            
            print(f"\n✅ Analysis complete!")
            print(f"Found {ai_count} new AI-related users out of {len(unanalyzed_users)} analyzed")
    
    if choice == "3" or choice == "2":
        # Show existing results
        print("\n📋 CURRENT RESULTS:")
        print("-" * 40)
        
        # Get updated stats
        db_stats = detector.db_manager.get_ai_stats()
        if db_stats:
            print(f"Total analyzed: {db_stats['total_analyzed']}")
            print(f"AI-related: {db_stats['ai_users']} ({db_stats['ai_percentage']}%)")
            print(f"Non-AI: {db_stats['non_ai_users']}")
            print()
            
            # Show sample AI users
            ai_users = detector.get_ai_users()
            print_sample_ai_users(ai_users, limit=15)
        else:
            print("No analysis results found.")
    
    elif choice == "4":
        # Show detailed statistics
        print("\n📊 DETAILED STATISTICS:")
        print("-" * 50)
        
        db_stats = detector.db_manager.get_stats()
        ai_stats = detector.db_manager.get_ai_stats()
        
        print(f"Database Overview:")
        print(f"  Total users: {db_stats['total_users']}")
        print(f"  Total relationships: {db_stats['total_relationships']}")
        print()
        
        if ai_stats:
            print(f"AI Analysis:")
            print(f"  Analyzed users: {ai_stats['total_analyzed']}")
            print(f"  AI-related: {ai_stats['ai_users']} ({ai_stats['ai_percentage']}%)")
            print(f"  Non-AI: {ai_stats['non_ai_users']}")
            print(f"  Keywords found: {ai_stats['total_keywords_found']} total")
            print(f"  Unique keywords: {ai_stats['unique_keywords']}")
            print()
            
            print_top_keywords(detector)
        else:
            print("No AI analysis data found.")
    
    elif choice == "5":
        # Test keyword detection
        test_bios = [
            "AI Researcher at OpenAI working on GPT models and safety alignment",
            "Machine Learning Engineer at Google, specializing in computer vision",
            "Data scientist building recommendation systems with deep learning",
            "Founder of AI startup focused on natural language processing solutions",
            "Software engineer passionate about tensorflow and pytorch development",
            "PhD student researching reinforcement learning and robotics at MIT",
            "Product manager at tech company, love hiking and photography",
            "Marketing specialist based in Miami, coffee enthusiast",
            "Freelance consultant helping businesses with predictive analytics",
            "Building the future with artificial intelligence and automation",
            "Regular person who enjoys travel, food, and good conversations",
            "CEO at SaaS company, building tools for developers worldwide"
        ]
        
        print("\n🧪 TESTING KEYWORD DETECTION:")
        print("-" * 50)
        detector.test_bio_detection(test_bios)
    
    else:
        print("❌ Invalid choice. Please run the script again.")
        return
    
    # Final options
    print("\n" + "=" * 60)
    print("🎉 Analysis complete!")
    
    # Offer to export results
    if input("\nWould you like to export AI users to a file? (y/n): ").lower() == 'y':
        export_ai_users(detector)
    
    print("\nThanks for using the AI Bio Analysis Tool! 🚀")

def export_ai_users(detector: AIKeywordDetector, filename: str = "ai_users_export.txt"):
    """Export AI users to a text file"""
    try:
        ai_users = detector.get_ai_users()
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("AI-Related Users Export\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {detector.db_manager.get_ai_stats().get('total_analyzed', 0)} users analyzed\n")
            f.write(f"AI Users Found: {len(ai_users)}\n\n")
            
            for i, user in enumerate(ai_users, 1):
                f.write(f"{i}. @{user['username']}\n")
                if user.get('name'):
                    f.write(f"   Name: {user['name']}\n")
                f.write(f"   Bio: {user['bio']}\n")
                f.write(f"   Keywords: {', '.join(user['found_keywords'])}\n")
                f.write(f"   Followers: {user.get('followers', 'N/A')}\n")
                f.write(f"   Following: {user.get('following', 'N/A')}\n")
                f.write("\n")
        
        print(f"✅ Exported {len(ai_users)} AI users to {filename}")
        
    except Exception as e:
        print(f"❌ Error exporting users: {e}")

def quick_analysis():
    """Quick analysis function for programmatic use"""
    detector = AIKeywordDetector()
    results = detector.analyze_all_users()
    
    return {
        'detector': detector,
        'results': results,
        'ai_users': detector.get_ai_users(),
        'stats': detector.db_manager.get_ai_stats()
    }

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user.")
        print("Progress has been saved to the database.")
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        print("Please check your database and configuration.")
