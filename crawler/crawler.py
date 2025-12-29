import logging
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Generator
from dataclasses import dataclass

from .config import CrawlerConfig
from .http_client import RedditHttpClient
from .parser import RedditParser
from .database import RedditDatabase
from .models import Post, Comment, CrawlState

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    subreddit: str
    posts_fetched: int
    posts_inserted: int
    posts_updated: int
    comments_fetched: int
    comments_inserted: int
    comments_updated: int
    errors: List[str]
    duration_seconds: float
    started_at: datetime
    completed_at: datetime


class RedditCrawler:
    
    def __init__(self, config: Optional[CrawlerConfig] = None):
        self.config = config or CrawlerConfig()

        self.http_client = RedditHttpClient(
            user_agent=self.config.user_agent,
            request_delay=self.config.request_delay,
            max_retries=self.config.max_retries,
            retry_delay=self.config.retry_delay,
            timeout=self.config.timeout
        )
        
        self.parser = RedditParser(include_deleted=self.config.include_deleted)
        self.database: Optional[RedditDatabase] = None
        
        # Setup logging
        self._setup_logging()
    
    def _setup_logging(self):
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_format))
        
        handlers = [console_handler]
        if self.config.log_file:
            file_handler = logging.FileHandler(self.config.log_file)
            file_handler.setFormatter(logging.Formatter(log_format))
            handlers.append(file_handler)
        
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            handlers=handlers
        )
    
    def connect_database(self) -> bool:
        self.database = RedditDatabase(self.config)
        return self.database.connect()
    
    def disconnect_database(self):
        if self.database:
            self.database.close()
    
    def _build_subreddit_url(
        self,
        subreddit: str,
        sort_by: str = "new",
        after: Optional[str] = None,
        limit: int = 100
    ) -> str:
        base = f"{self.config.base_url}/r/{subreddit}/{sort_by}.json"
        params = [f"limit={min(limit, 100)}"]  # Reddit max is 100 per page
        
        if after:
            params.append(f"after={after}")
        
        if sort_by == "top" and self.config.time_filter:
            params.append(f"t={self.config.time_filter}")
        
        return f"{base}?{'&'.join(params)}"
    
    def _build_post_url(self, permalink: str) -> str:
        if not permalink.startswith("/"):
            permalink = f"/{permalink}"
        return f"{self.config.base_url}{permalink}.json?limit={self.config.max_comments_per_post}&depth={self.config.comment_depth}"
    
    def fetch_posts(
        self,
        subreddit: Optional[str] = None,
        max_posts: Optional[int] = None,
        sort_by: Optional[str] = None
    ) -> Generator[Post, None, None]:
        # Fetch posts from a subreddit.
        subreddit = subreddit or self.config.subreddit
        max_posts = max_posts or self.config.max_posts
        sort_by = sort_by or self.config.sort_by
        
        logger.info(f"Fetching posts from r/{subreddit} (sort: {sort_by}, max: {max_posts})")
        
        after_token = None
        posts_fetched = 0
        
        while posts_fetched < max_posts:
            # Calculate how many more posts we need
            remaining = max_posts - posts_fetched
            batch_size = min(remaining, 100)
            
            # Build URL and fetch
            url = self._build_subreddit_url(subreddit, sort_by, after_token, batch_size)
            logger.debug(f"Fetching: {url}")
            
            response_data, error = self.http_client.get_json(url)
            
            if error:
                logger.error(f"Error fetching posts: {error}")
                break
            
            # Parse posts
            posts, after_token = self.parser.parse_post_listing(response_data)
            
            if not posts:
                logger.info("No more posts to fetch")
                break
            
            for post in posts:
                if posts_fetched >= max_posts:
                    break
                yield post
                posts_fetched += 1
            
            # Check if there are more pages
            if not after_token:
                break
            
            logger.info(f"Fetched {posts_fetched}/{max_posts} posts")
        
        logger.info(f"Completed fetching {posts_fetched} posts from r/{subreddit}")
    
    def fetch_comments(self, post: Post) -> List[Comment]:
        # Fetch all comments for a post.

        logger.debug(f"Fetching comments for post: {post.post_id}")
        
        url = self._build_post_url(post.permalink)
        response_data, error = self.http_client.get_json(url)
        
        if error:
            logger.error(f"Error fetching comments for {post.post_id}: {error}")
            return []
        
        updated_post, comments = self.parser.parse_comments_page(
            response_data,
            post.post_id,
            post.subreddit,
            self.config.comment_depth
        )
        
        logger.debug(f"Fetched {len(comments)} comments for post {post.post_id}")
        return comments
    
    def crawl(
        self,
        subreddit: Optional[str] = None,
        max_posts: Optional[int] = None,
        fetch_comments: bool = True,
        resume: bool = False
    ) -> CrawlResult:
        subreddit = subreddit or self.config.subreddit
        max_posts = max_posts or self.config.max_posts
        
        start_time = datetime.utcnow()
        errors = []
        
        # Ensure database is connected
        if not self.database:
            if not self.connect_database():
                return CrawlResult(
                    subreddit=subreddit,
                    posts_fetched=0,
                    posts_inserted=0,
                    posts_updated=0,
                    comments_fetched=0,
                    comments_inserted=0,
                    comments_updated=0,
                    errors=["Failed to connect to database"],
                    duration_seconds=0,
                    started_at=start_time,
                    completed_at=datetime.utcnow()
                )
        
        self.database.reset_stats()
        self.parser.reset_stats()
  
        crawl_state = None
        if resume:
            crawl_state = self.database.get_crawl_state(subreddit)
            if crawl_state:
                logger.info(f"Resuming crawl from previous state: {crawl_state.posts_crawled} posts")
        
        if not crawl_state:
            crawl_state = CrawlState(subreddit=subreddit)
        
        logger.info(f"Starting crawl of r/{subreddit} (max_posts={max_posts}, fetch_comments={fetch_comments})")
        
        posts_processed = 0
        comments_processed = 0
        
        try:
            for post in self.fetch_posts(subreddit, max_posts):
                was_updated, action = self.database.upsert_post(post)
                posts_processed += 1
                
                if action == "error":
                    errors.append(f"Failed to upsert post {post.post_id}")
                    continue
                
                if fetch_comments and self.config.fetch_all_comments:
                    try:
                        comments = self.fetch_comments(post)
                        
                        for comment in comments:
                            _, comment_action = self.database.upsert_comment(comment)
                            comments_processed += 1
                            
                            if comment_action == "error":
                                errors.append(f"Failed to upsert comment {comment.comment_id}")
                        
                    except Exception as e:
                        logger.error(f"Error fetching comments for {post.post_id}: {e}")
                        errors.append(f"Comment fetch error for {post.post_id}: {str(e)}")

                if posts_processed % 10 == 0:
                    crawl_state.posts_crawled = posts_processed
                    crawl_state.comments_crawled = comments_processed
                    crawl_state.last_post_id = post.post_id
                    crawl_state.last_activity = datetime.utcnow()
                    self.database.save_crawl_state(crawl_state)
                    
                    logger.info(f"Progress: {posts_processed} posts, {comments_processed} comments")
            
            # Mark crawl as complete
            crawl_state.is_complete = True
            crawl_state.posts_crawled = posts_processed
            crawl_state.comments_crawled = comments_processed
            self.database.save_crawl_state(crawl_state)
            
        except KeyboardInterrupt:
            logger.warning("Crawl interrupted by user")
            errors.append("Crawl interrupted by user")
            # Save state for resume
            crawl_state.posts_crawled = posts_processed
            crawl_state.comments_crawled = comments_processed
            self.database.save_crawl_state(crawl_state)
        
        except Exception as e:
            logger.error(f"Crawl error: {e}")
            errors.append(f"Crawl error: {str(e)}")
        
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()
        
        # Get database stats
        db_stats = self.database.get_stats()
        
        result = CrawlResult(
            subreddit=subreddit,
            posts_fetched=posts_processed,
            posts_inserted=db_stats["posts_inserted"],
            posts_updated=db_stats["posts_updated"],
            comments_fetched=comments_processed,
            comments_inserted=db_stats["comments_inserted"],
            comments_updated=db_stats["comments_updated"],
            errors=errors,
            duration_seconds=duration,
            started_at=start_time,
            completed_at=end_time
        )
        
        logger.info(f"Crawl completed in {duration:.2f}s")
        logger.info(f"Posts: {result.posts_inserted} inserted, {result.posts_updated} updated")
        logger.info(f"Comments: {result.comments_inserted} inserted, {result.comments_updated} updated")
        
        if errors:
            logger.warning(f"Crawl completed with {len(errors)} errors")
        
        return result
    
    def crawl_incremental(
        self,
        subreddit: Optional[str] = None,
        max_posts: int = 50
    ) -> CrawlResult:
        logger.info("Starting incremental crawl...")
        
        # Crawl with "new" sorting to catch updates
        return self.crawl(
            subreddit=subreddit,
            max_posts=max_posts,
            fetch_comments=True,
            resume=False
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get combined crawler statistics."""
        stats = {
            "http": self.http_client.get_stats(),
            "parser": self.parser.get_stats()
        }
        
        if self.database:
            stats["database"] = self.database.get_stats()
        
        return stats


class CrawlerScheduler:
    
    def __init__(self, crawler: RedditCrawler, interval_minutes: int = 30):
        self.crawler = crawler
        self.interval_seconds = interval_minutes * 60
        self.running = False
    
    def start(self, subreddit: Optional[str] = None, max_posts: int = 50):
        """Start the scheduler loop."""
        self.running = True
        logger.info(f"Starting scheduler with {self.interval_seconds}s interval")
        
        while self.running:
            try:
                result = self.crawler.crawl_incremental(subreddit, max_posts)
                
                logger.info(
                    f"Scheduled crawl complete: "
                    f"{result.posts_inserted} new posts, "
                    f"{result.posts_updated} updated, "
                    f"{result.comments_inserted} new comments"
                )
                
            except Exception as e:
                logger.error(f"Scheduled crawl error: {e}")
            
            if self.running:
                logger.info(f"Sleeping for {self.interval_seconds}s until next crawl...")
                time.sleep(self.interval_seconds)
    
    def stop(self):
        # Stop the scheduler.
        self.running = False
        logger.info("Scheduler stopped")

