
import sys
import os
from config import config

project_root = os.path.dirname(os.getcwd())
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# import the crawler
from crawler.crawler import RedditCrawler


crawler = RedditCrawler(config)

# Connect to PostgreSQL (make sure PostgreSQL is running!)
if crawler.connect_database():
    print("✅ Connected to PostgreSQL!")
else:
    print("❌ Failed to connect. Make sure PostgreSQL is running and the database exists.")
    print("   Create database with: createdb reddit_crawler")



# Run the crawl
result = crawler.crawl(fetch_comments=True)

print(f"Duration: {result.duration_seconds:.2f}s")
print(f"Posts fetched: {result.posts_fetched}")
print(f"--New: {result.posts_inserted}")
print(f"--Updated: {result.posts_updated}")
print(f"Comments fetched: {result.comments_fetched}")
print(f"--New: {result.comments_inserted}")
print(f"--Updated: {result.comments_updated}")
