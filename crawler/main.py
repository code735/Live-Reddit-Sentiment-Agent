import argparse
import json
import sys
import signal
from datetime import datetime
from typing import Optional

from .config import CrawlerConfig
from .crawler import RedditCrawler, CrawlerScheduler


def parse_args():
    parser = argparse.ArgumentParser(
        description="Reddit Crawler for ML Data Collection",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full crawl
  python -m crawler.main --subreddit IndianStockMarket --max-posts 100
  
  # Incremental crawl for updates
  python -m crawler.main --incremental --max-posts 50
  
  # Scheduled crawling every 30 minutes
  python -m crawler.main --scheduled --interval 30
  
  # Export collected data
  python -m crawler.main --export --output data.json
        """
    )
    
    # Target configuration
    parser.add_argument(
        "--subreddit", "-s",
        default="IndianStockMarket",
        help="Subreddit to crawl (default: IndianStockMarket)"
    )
    
    # Crawl settings
    parser.add_argument(
        "--max-posts", "-n",
        type=int,
        default=100,
        help="Maximum number of posts to fetch (default: 100)"
    )
    
    parser.add_argument(
        "--max-comments",
        type=int,
        default=500,
        help="Maximum comments per post (default: 500)"
    )
    
    parser.add_argument(
        "--comment-depth",
        type=int,
        default=10,
        help="Maximum comment thread depth (default: 10)"
    )
    
    parser.add_argument(
        "--sort",
        choices=["new", "hot", "top", "rising"],
        default="new",
        help="Post sorting method (default: new)"
    )
    
    parser.add_argument(
        "--time-filter",
        choices=["hour", "day", "week", "month", "year", "all"],
        default="all",
        help="Time filter for 'top' sort (default: all)"
    )
    
    # Operation modes
    parser.add_argument(
        "--incremental", "-i",
        action="store_true",
        help="Perform incremental crawl (check for updates)"
    )
    
    parser.add_argument(
        "--scheduled",
        action="store_true",
        help="Run scheduled continuous crawling"
    )
    
    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Minutes between scheduled crawls (default: 30)"
    )
    
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from previous crawl state"
    )
    
    parser.add_argument(
        "--no-comments",
        action="store_true",
        help="Skip fetching comments"
    )
    
    # Database settings
    parser.add_argument(
        "--mongo-uri",
        default="mongodb://localhost:27017",
        help="MongoDB connection URI"
    )
    
    parser.add_argument(
        "--database",
        default="reddit_crawler",
        help="MongoDB database name (default: reddit_crawler)"
    )
    
    # Export mode
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export collected data instead of crawling"
    )
    
    parser.add_argument(
        "--output", "-o",
        help="Output file for export (default: stdout)"
    )
    
    parser.add_argument(
        "--format",
        choices=["json", "jsonl"],
        default="json",
        help="Export format (default: json)"
    )
    
    # Other options
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Seconds between requests (default: 2.0)"
    )
    
    parser.add_argument(
        "--include-deleted",
        action="store_true",
        help="Include deleted posts and comments"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose logging"
    )
    
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics"
    )
    
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Reset crawl state for the subreddit"
    )
    
    return parser.parse_args()


def create_config(args) -> CrawlerConfig:
    return CrawlerConfig(
        subreddit=args.subreddit,
        max_posts=args.max_posts,
        max_comments_per_post=args.max_comments,
        comment_depth=args.comment_depth,
        sort_by=args.sort,
        time_filter=args.time_filter,
        request_delay=args.delay,
        mongo_uri=args.mongo_uri,
        database_name=args.database,
        include_deleted=args.include_deleted,
        fetch_all_comments=not args.no_comments,
        log_level="DEBUG" if args.verbose else "INFO"
    )


def export_data(crawler: RedditCrawler, args):
    if not crawler.database:
        print("Error: Database not connected", file=sys.stderr)
        return
    
    print(f"Exporting data from r/{args.subreddit}...", file=sys.stderr)
    
    data = crawler.database.export_for_ml(
        subreddit=args.subreddit,
        include_comments=True,
        flatten=False
    )
    
    def serialize_dates(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
    
    output = None
    if args.output:
        output = open(args.output, "w")
    
    try:
        if args.format == "json":
            json.dump(
                data,
                output or sys.stdout,
                default=serialize_dates,
                indent=2
            )
        else:  # jsonl
            for item in data:
                json.dump(item, output or sys.stdout, default=serialize_dates)
                print(file=output or sys.stdout)
    finally:
        if output:
            output.close()
    
    print(f"\nExported {len(data)} posts", file=sys.stderr)


def show_stats(crawler: RedditCrawler):
    if not crawler.database:
        print("Error: Database not connected", file=sys.stderr)
        return
    
    stats = crawler.database.get_stats()
    
    print("\n" + "="*50)
    print("DATABASE STATISTICS")
    print("="*50)
    print(f"Total Posts:     {stats.get('total_posts', 0):,}")
    print(f"Total Comments:  {stats.get('total_comments', 0):,}")
    print(f"Total Changes:   {stats.get('total_changes', 0):,}")
    print("="*50 + "\n")


def run_crawl(crawler: RedditCrawler, args):
    print(f"\n{'='*50}")
    print(f"REDDIT CRAWLER")
    print(f"{'='*50}")
    print(f"Target:        r/{args.subreddit}")
    print(f"Max Posts:     {args.max_posts}")
    print(f"Sort By:       {args.sort}")
    print(f"Fetch Comments: {not args.no_comments}")
    print(f"{'='*50}\n")
    
    if args.incremental:
        result = crawler.crawl_incremental(
            subreddit=args.subreddit,
            max_posts=args.max_posts
        )
    else:
        result = crawler.crawl(
            subreddit=args.subreddit,
            max_posts=args.max_posts,
            fetch_comments=not args.no_comments,
            resume=args.resume
        )
    
    print(f"\n{'='*50}")
    print("CRAWL RESULTS")
    print(f"{'='*50}")
    print(f"Duration:          {result.duration_seconds:.2f}s")
    print(f"Posts Fetched:     {result.posts_fetched}")
    print(f"Posts Inserted:    {result.posts_inserted}")
    print(f"Posts Updated:     {result.posts_updated}")
    print(f"Comments Fetched:  {result.comments_fetched}")
    print(f"Comments Inserted: {result.comments_inserted}")
    print(f"Comments Updated:  {result.comments_updated}")
    
    if result.errors:
        print(f"\nErrors ({len(result.errors)}):")
        for error in result.errors[:5]:  
            print(f"  - {error}")
        if len(result.errors) > 5:
            print(f"  ... and {len(result.errors) - 5} more")
    
    print(f"{'='*50}\n")
    
    return result


def run_scheduled(crawler: RedditCrawler, args):
    scheduler = CrawlerScheduler(crawler, interval_minutes=args.interval)
    
    def signal_handler(sig, frame):
        print("\nStopping scheduler...")
        scheduler.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print(f"\nStarting scheduled crawling...")
    print(f"Subreddit: r/{args.subreddit}")
    print(f"Interval: {args.interval} minutes")
    print(f"Press Ctrl+C to stop\n")
    
    scheduler.start(
        subreddit=args.subreddit,
        max_posts=args.max_posts
    )


def main():
    args = parse_args()
    
    config = create_config(args)
    
    crawler = RedditCrawler(config)
    
    if not crawler.connect_database():
        print("Failed to connect to MongoDB. Is it running?", file=sys.stderr)
        print(f"URI: {config.mongo_uri}", file=sys.stderr)
        sys.exit(1)
    
    try:
        if args.reset_state:
            crawler.database.reset_crawl_state(args.subreddit)
            print(f"Reset crawl state for r/{args.subreddit}")
        
        if args.stats:
            show_stats(crawler)
        elif args.export:
            export_data(crawler, args)
        elif args.scheduled:
            run_scheduled(crawler, args)
        else:
            run_crawl(crawler, args)
    
    finally:
        crawler.disconnect_database()


if __name__ == "__main__":
    main()

