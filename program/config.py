import os

class Config:
    # Database
    DATABASE_PATH = "social_data.db"
    
    # API
    API_BASE_URL = os.getenv("API_BASE_URL", "")

    API_TOKEN = os.getenv("API_TOKEN", "")
    print("API_BASE_URL", API_BASE_URL)
    print("API_TOKEN",API_TOKEN)
    API_HEADERS = {
        "X-API-Key": API_TOKEN,
        "Content-Type": "application/json"
    }
    
    # Rate limiting
    REQUEST_DELAY = 1.0
    MAX_PAGES_PER_USER = 10
    
    # URLs
    PINNED_POST_URL_TEMPLATE = "https://twitter.com/i/web/status/{tweet_id}"
