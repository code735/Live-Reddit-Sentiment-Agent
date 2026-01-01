# Create configuration
from crawler.crawler import CrawlerConfig

config = CrawlerConfig(
    subreddit="IndianStockMarket",
    max_posts=30,
    max_comments_per_post=100,
    sort_by="new",  # Options: "new", "hot", "top", "rising"
    request_delay=2.0,
    # PostgreSQL settings
    postgres_host="localhost",
    postgres_port=5432,
    postgres_user="postgres",
    postgres_password="livredsentagent123",
    postgres_database="livredsentagentdb"
)