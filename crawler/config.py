from dataclasses import dataclass, field
from typing import Optional
import os


@dataclass
class CrawlerConfig:   
    
    subreddit: str = "IndianStockMarket"
    base_url: str = "https://www.reddit.com"
    
    max_posts: int = 100  # Maximum number of posts to fetch per crawl
    max_comments_per_post: int = 500  # Maximum comments to fetch per post
    comment_depth: int = 10  # How deep to go into comment threads
    sort_by: str = "new"  # Options: "hot", "new", "top", "rising"
    time_filter: str = "all"  # For "top": "hour", "day", "week", "month", "year", "all"
    
    request_delay: float = 2.0  # Seconds between requests
    max_retries: int = 3  # Number of retries on failure
    retry_delay: float = 5.0  # Seconds to wait before retry
    timeout: int = 30  # Request timeout in seconds
    
    user_agent: str = "RedditCrawler/1.0 (ML Research Project; Contact: your@email.com)"
    
    # MongoDB settings
    mongo_uri: str = field(default_factory=lambda: os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    database_name: str = "reddit_crawler"
    
    update_existing: bool = True  # Update posts/comments if they changed
    fetch_all_comments: bool = True  # Fetch full comment trees
    include_deleted: bool = False  # Include [deleted] posts/comments
    
    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = "crawler.log"


@dataclass
class SubredditTarget:
    name: str
    max_posts: Optional[int] = None
    sort_by: Optional[str] = None
    enabled: bool = True


# Default
default_config = CrawlerConfig()

