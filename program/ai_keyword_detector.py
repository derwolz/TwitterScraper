import re
from typing import List, Dict, Set
from database_manager import DatabaseManager
from config import Config

class AIKeywordDetector:
    """Detects AI-related keywords in user bios"""
    
    def __init__(self):
        self.db_manager = DatabaseManager(Config.DATABASE_PATH)
        
        # Define AI-related keywords and phrases
        self.ai_keywords = {
            # Core AI terms
            'artificial intelligence', 'ai', 'machine learning', 'ml', 'deep learning',
            'neural network', 'neural networks', 'computer vision', 'nlp', 
            'natural language processing', 'data science', 'data scientist',
            
            # AI roles and job titles
            'ai engineer', 'ml engineer', 'machine learning engineer', 'ai researcher',
            'ai scientist', 'data scientist', 'ml researcher', 'ai specialist',
            'ai developer', 'ml developer', 'ai consultant', 'ai architect',
            
            # AI technologies and frameworks
            'tensorflow', 'pytorch', 'keras', 'scikit-learn', 'opencv', 'transformers',
            'hugging face', 'openai', 'anthropic', 'claude', 'gpt', 'chatgpt',
            'large language model', 'llm', 'generative ai', 'gen ai', 'stable diffusion',
            
            # AI applications
            'computer vision', 'image recognition', 'speech recognition', 'recommender systems',
            'recommendation engine', 'chatbot', 'virtual assistant', 'automated',
            'automation', 'predictive analytics', 'predictive modeling',
            
            # AI research areas
            'reinforcement learning', 'supervised learning', 'unsupervised learning',
            'transfer learning', 'few-shot learning', 'zero-shot learning',
            'fine-tuning', 'model training', 'ai safety', 'ai alignment',
            'ai ethics', 'responsible ai', 'explainable ai', 'xai',
            
            # AI companies and organizations
            'openai', 'anthropic', 'deepmind', 'google ai', 'microsoft ai',
            'nvidia ai', 'meta ai', 'apple ai', 'amazon ai', 'tesla ai',
            
            # AI events and communities
            'neurips', 'icml', 'iclr', 'aaai', 'ai conference', 'ml conference',
            'ai meetup', 'ml meetup', 'ai community', 'ml community'
        }
        
        # Words that might be false positives (to be more careful about)
        self.false_positive_indicators = {
            'ai weiwei', 'taipei', 'thailand', 'hawaii', 'miami', 'haiti',
            'railway', 'airline', 'available', 'ailing', 'air', 'aid'
        }
    
    def preprocess_bio(self, bio: str) -> str:
        """Clean and preprocess bio text for keyword matching"""
        if not bio:
            return ""
        
        # Convert to lowercase
        bio = bio.lower()
        
        # Remove URLs
        bio = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', bio)
        
        # Remove email addresses
        bio = re.sub(r'\S+@\S+', '', bio)
        
        # Replace common separators with spaces
        bio = re.sub(r'[|•·/\\]', ' ', bio)
        
        # Remove extra whitespace
        bio = re.sub(r'\s+', ' ', bio).strip()
        
        return bio
    
    def contains_ai_keywords(self, bio: str) -> tuple[bool, List[str]]:
        """
        Check if bio contains AI-related keywords
        Returns: (is_ai_related, list_of_found_keywords)
        """
        if not bio:
            return False, []
        
        processed_bio = self.preprocess_bio(bio)
        found_keywords = []
        
        # Check for false positives first
        for fp_word in self.false_positive_indicators:
            if fp_word in processed_bio:
                # If we find a false positive, be more strict about AI keywords
                pass
        
        # Check for AI keywords
        for keyword in self.ai_keywords:
            # Use word boundaries for single words, phrase matching for multi-word terms
            if ' ' in keyword:
                # Multi-word phrase
                if keyword in processed_bio:
                    found_keywords.append(keyword)
            else:
                # Single word - use word boundaries to avoid partial matches
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, processed_bio):
                    found_keywords.append(keyword)
        
        # Additional context-based checks
        ai_context_patterns = [
            r'\bworking (on|with|in) ai\b',
            r'\bbuilding ai\b',
            r'\bai at\b',
            r'\bai @\b',
            r'\b(founder|ceo|cto) at .*(ai|ml)\b',
            r'\b(ai|ml) (startup|company)\b',
            r'\bresearch(ing)? (ai|ml)\b'
        ]
        
        for pattern in ai_context_patterns:
            if re.search(pattern, processed_bio):
                found_keywords.append(f"context_pattern: {pattern}")
        
        return len(found_keywords) > 0, found_keywords
    
    def analyze_user_bio(self, username: str) -> Dict:
        """Analyze a single user's bio for AI keywords"""
        user_data = self.db_manager.get_user(username)
        
        if not user_data:
            return {"error": f"User {username} not found in database"}
        
        bio = user_data.get('bio', '')
        is_ai_related, keywords = self.contains_ai_keywords(bio)
        
        result = {
            "username": username,
            "bio": bio,
            "is_ai_related": is_ai_related,
            "found_keywords": keywords,
            "keyword_count": len(keywords)
        }
        
        # Store the result
        self.db_manager.store_ai_analysis(username, is_ai_related, keywords)
        
        return result
    
    def analyze_all_users(self) -> Dict[str, any]:
        """Analyze all users in the database for AI keywords"""
        print("Starting AI keyword analysis for all users...")
        
        users = self.db_manager.get_all_users()
        total_users = len(users)
        ai_users = 0
        results = []
        
        print(f"Found {total_users} users to analyze")
        
        for i, user in enumerate(users, 1):
            username = user['username']
            print(f"Analyzing {i}/{total_users}: {username}")
            
            result = self.analyze_user_bio(username)
            if not result.get('error') and result.get('is_ai_related'):
                ai_users += 1
                print(f"  ✓ AI-related! Keywords: {result['found_keywords'][:3]}...")
            
            results.append(result)
        
        summary = {
            "total_analyzed": total_users,
            "ai_related_users": ai_users,
            "ai_percentage": round((ai_users / total_users * 100) if total_users > 0 else 0, 2),
            "results": results
        }
        
        print(f"\nAnalysis complete!")
        print(f"Total users analyzed: {total_users}")
        print(f"AI-related users found: {ai_users} ({summary['ai_percentage']}%)")
        
        return summary
    
    def get_ai_users(self) -> List[Dict]:
        """Get all users marked as AI-related"""
        return self.db_manager.get_ai_users()
    
    def get_non_ai_users(self) -> List[Dict]:
        """Get all users marked as non-AI-related"""
        return self.db_manager.get_non_ai_users()
    
    def update_keywords(self, new_keywords: Set[str]):
        """Add new keywords to the detection set"""
        self.ai_keywords.update(new_keywords)
        print(f"Added {len(new_keywords)} new keywords")
    
    def test_bio_detection(self, test_bios: List[str]):
        """Test the keyword detection on sample bios"""
        print("Testing AI keyword detection:")
        print("-" * 50)
        
        for i, bio in enumerate(test_bios, 1):
            is_ai, keywords = self.contains_ai_keywords(bio)
            print(f"Test {i}: {'✓ AI' if is_ai else '✗ Not AI'}")
            print(f"Bio: {bio[:80]}...")
            if keywords:
                print(f"Found: {keywords[:5]}")
            print()

if __name__ == "__main__":
    # Test bios for validation
    test_bios = [
        "AI Researcher at OpenAI. Working on large language models and AI safety.",
        "Software engineer passionate about machine learning and deep learning applications.",
        "Founder of AI startup building computer vision solutions for healthcare.",
        "Data scientist specializing in NLP and recommendation systems at Google.",
        "Regular person who likes hiking and photography. Based in Hawaii.",
        "Building the future with artificial intelligence. Former ML engineer at Tesla.",
        "PhD in Computer Science. Research focus: reinforcement learning and robotics.",
        "Marketing manager at a tech company. Love traveling and coffee.",
        "AI/ML consultant helping businesses implement predictive analytics solutions.",
        "Just someone who enjoys good food and great conversations. Living life!"
    ]
    
    detector = AIKeywordDetector()
    
    # Test the detection logic
    detector.test_bio_detection(test_bios)
    
    # Analyze all users in database
    # results = detector.analyze_all_users()
