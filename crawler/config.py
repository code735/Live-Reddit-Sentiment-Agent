from dataclasses import dataclass, field
from typing import Optional
import os


@dataclass
class CrawlerConfig:   
    
    subreddit: str = "wallstreetbets"
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
    
    # PostgreSQL settings
    postgres_host: str = field(default_factory=lambda: os.getenv("POSTGRES_HOST", "localhost"))
    postgres_port: int = field(default_factory=lambda: int(os.getenv("POSTGRES_PORT", "5432")))
    postgres_user: str = field(default_factory=lambda: os.getenv("POSTGRES_USER", "postgres"))
    postgres_password: str = field(default_factory=lambda: os.getenv("POSTGRES_PASSWORD", "postgres"))
    postgres_database: str = field(default_factory=lambda: os.getenv("POSTGRES_DATABASE", "reddit_crawler"))
    
    update_existing: bool = True  # Update posts/comments if they changed
    fetch_all_comments: bool = True  # Fetch full comment trees
    include_deleted: bool = False  # Include [deleted] posts/comments
    
    # Logging
    log_level: str = "INFO"
    log_file: Optional[str] = "crawler.log"
    
    @property
    def postgres_dsn(self) -> str:
        """Get PostgreSQL connection string."""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}"


@dataclass
class SubredditTarget:
    name: str
    max_posts: Optional[int] = None
    sort_by: Optional[str] = None
    enabled: bool = True


# Default
default_config = CrawlerConfig()
