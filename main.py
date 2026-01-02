import os
import json
import uuid
import hashlib
import argparse
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict

from dotenv import load_dotenv
from google import genai
from google.genai import types

from config import config
from crawler.crawler import RedditCrawler
from crawler.database import RedditDatabase
from sentiment.app.services.sentiment import SentimentService


# System prompt for stock ticker extraction
TICKER_EXTRACTION_PROMPT = """You are a financial text analysis expert. Your task is to identify stock tickers mentioned in Reddit posts and comments.

Given a text about stocks/trading, extract the stock ticker symbol(s) being discussed.

Rules:
1. Return ONLY valid stock ticker symbols (e.g., TSLA, AAPL, GME, AMC, NVDA)
2. If the text mentions company names, convert them to their ticker symbols
3. Common mappings: Tesla=TSLA, Apple=AAPL, Microsoft=MSFT, Amazon=AMZN, Google/Alphabet=GOOGL, Meta/Facebook=META, NVIDIA=NVDA
4. If multiple tickers are mentioned, return the PRIMARY one being discussed
5. If no clear stock ticker is found, return "UNKNOWN"
6. Return ONLY the ticker symbol, nothing else (e.g., "TSLA" not "The ticker is TSLA")

Examples:
- "TSLA to the moon! 🚀" -> TSLA
- "Tesla is going to crush earnings" -> TSLA
- "Bought some GME and AMC today, but GME is my main play" -> GME
- "The market is crazy today" -> UNKNOWN
- "TCS looking bullish, Tata Motors also strong" -> TCS
"""


@dataclass
class CommentEvent:
    user_name: str
    text: str


@dataclass
class StockEvent:
    event_id: str
    post_id: str
    event_type: str
    ticker: str
    text: str 
    post_des: str 
    comments: List[Dict[str, str]]
    sentiment: str
    confidence: float
    source: str
    created_at: str
    observed_at: str
    url: str
    author: str
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class StockSentimentAnalyzer:
    
    def __init__(self, gemini_api_key: Optional[str] = None):
        self.api_key = gemini_api_key or os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY is required. Set it in environment or pass it to the constructor.")
        
        self.gemini_client = genai.Client(api_key=self.api_key)
        self.sentiment_service = SentimentService()
        self.crawler = RedditCrawler(config)
        
        # Connect to database
        if not self.crawler.connect_database():
            raise ConnectionError("Failed to connect to database")
    
    def extract_ticker(self, text: str) -> str:
        try:
            messages = [
                types.Content(
                    role="user", 
                    parts=[types.Part(text=f"Extract the stock ticker from this text:\n\n{text[:2000]}")]  # Limit text length
                )
            ]
            
            response = self.gemini_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=messages,
                config=types.GenerateContentConfig(
                    system_instruction=TICKER_EXTRACTION_PROMPT,
                    temperature=0.1,
                    max_output_tokens=20
                )
            )
            
            if response and response.text:
                ticker = response.text.strip().upper()
                # Clean up the ticker - remove any extra text
                ticker = ticker.split()[0] if ticker else "UNKNOWN"
                # Validate it looks like a ticker (1-5 uppercase letters)
                if ticker and 1 <= len(ticker) <= 5 and ticker.isalpha():
                    return ticker
            
            return "UNKNOWN"
            
        except Exception as e:
            print(f"Error extracting ticker: {e}")
            return "UNKNOWN"
    
    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        try:
            if not text or len(text.strip()) < 5:
                return {"label": "neutral", "confidence": 0.5}
            
            result = self.sentiment_service.analyze(text[:512])  # FinBERT has token limits
            return {
                "label": result["label"].lower(),
                "confidence": round(result["confidence"], 4)
            }
        except Exception as e:
            print(f"Error analyzing sentiment: {e}")
            return {"label": "neutral", "confidence": 0.5}
    
    def generate_event_id(self, post_id: str, observed_at: str) -> str:
        unique_string = f"{post_id}_{observed_at}"
        return hashlib.md5(unique_string.encode()).hexdigest()
    
    def process_post(self, post: Dict[str, Any], comments: List[Dict[str, Any]]) -> StockEvent:
        observed_at = datetime.utcnow().isoformat() + "Z"
        
        # Combine title and selftext for ticker extraction
        full_text = f"{post.get('title', '')} {post.get('selftext', '')}"
        
        # Extract ticker using Gemini
        ticker = self.extract_ticker(full_text)
        
        # Analyze sentiment using FinBERT
        sentiment_result = self.analyze_sentiment(full_text)
        
        # Process comments
        comment_list = []
        for comment in comments[:20]:  # Limit to first 20 comments
            if comment.get('author') and comment.get('body'):
                comment_list.append({
                    "user_name": comment.get('author', '[deleted]'),
                    "text": comment.get('body', '')[:500]  # Limit comment length
                })
        
        # Generate event ID
        event_id = self.generate_event_id(post['post_id'], observed_at)
        
        # Format created_at timestamp
        created_at = post.get('created_utc')
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat() + "Z"
        elif created_at is None:
            created_at = observed_at
        else:
            created_at = str(created_at)
        
        # Construct URL
        permalink = post.get('permalink', '')
        url = f"https://old.reddit.com{permalink}" if permalink else ""
        
        return StockEvent(
            event_id=event_id,
            post_id=post['post_id'],
            event_type="upsert",
            ticker=ticker,
            text=post.get('title', ''),
            post_des=post.get('selftext', '') or '',
            comments=comment_list,
            sentiment=sentiment_result["label"],
            confidence=sentiment_result["confidence"],
            source="reddit",
            created_at=created_at,
            observed_at=observed_at,
            url=url,
            author=post.get('author', '[deleted]')
        )
    
    def fetch_and_process_posts(
        self, 
        subreddit: Optional[str] = None,
        limit: int = 30,
        output_file: Optional[str] = None,
        verbose: bool = False
    ) -> List[StockEvent]:
        subreddit = subreddit or config.subreddit
        
        if verbose:
            print(f"Fetching posts from r/{subreddit}...")
        
        # Get posts from database
        posts = self.crawler.database.get_posts(
            subreddit=subreddit,
            limit=limit,
            sort_by="created_utc",
            sort_order="DESC"
        )
        
        if verbose:
            print(f"Found {len(posts)} posts to process")
        
        events = []
        
        for i, post in enumerate(posts):
            if verbose:
                print(f"Processing post {i+1}/{len(posts)}: {post.get('title', '')[:50]}...")
            
            # Get comments for this post
            comments = self.crawler.database.get_comments_for_post(
                post_id=post['post_id'],
                limit=config.max_comments_per_post
            )
            
            # Process the post
            event = self.process_post(post, comments)
            events.append(event)
            
            if verbose:
                print(f"  Ticker: {event.ticker}, Sentiment: {event.sentiment} ({event.confidence:.2%})")
        
        # Write to output file if specified
        if output_file:
            self.write_events_to_jsonl(events, output_file)
            if verbose:
                print(f"\nEvents written to {output_file}")
        
        return events
    
    def crawl_and_process(
        self,
        subreddit: Optional[str] = None,
        max_posts: int = 30,
        output_file: Optional[str] = None,
        verbose: bool = False
    ) -> List[StockEvent]:
        subreddit = subreddit or config.subreddit
        
        if verbose:
            print(f"Crawling r/{subreddit} for new posts...")
        
        # Crawl new posts
        result = self.crawler.crawl(
            subreddit=subreddit,
            max_posts=max_posts,
            fetch_comments=True,
            resume=False
        )
        
        if verbose:
            print(f"Crawl complete: {result.posts_fetched} posts, {result.comments_fetched} comments")
            if result.errors:
                print(f"Errors: {result.errors}")
        
        # Now process the posts from database
        return self.fetch_and_process_posts(
            subreddit=subreddit,
            limit=max_posts,
            output_file=output_file,
            verbose=verbose
        )
    
    def write_events_to_jsonl(self, events: List[StockEvent], output_file: str):
        with open(output_file, 'a', encoding='utf-8') as f:
            for event in events:
                f.write(json.dumps(event.to_dict(), ensure_ascii=False) + '\n')
    
    def close(self):
        if self.crawler:
            self.crawler.disconnect_database()


def main():
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="Reddit Stock Sentiment Analyzer - Analyze Reddit posts for stock sentiment using Gemini and FinBERT"
    )
    parser.add_argument(
        "--mode", 
        type=str, 
        choices=["fetch", "crawl"], 
        default="fetch",
        help="Mode: 'fetch' to process existing posts from DB, 'crawl' to fetch new posts first"
    )
    parser.add_argument(
        "--subreddit", 
        type=str, 
        default=None,
        help=f"Subreddit to analyze (default: {config.subreddit})"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        default=30,
        help="Maximum number of posts to process (default: 30)"
    )
    parser.add_argument(
        "--output", 
        type=str, 
        default=None,
        help="Output JSONL file path (default: pathway_streams/data_stream/events_latest.jsonl)"
    )
    parser.add_argument(
        "--verbose", 
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--print-json",
        action="store_true",
        help="Print events as JSON to stdout"
    )
    
    args = parser.parse_args()
    
    output_file = args.output or "pathway_streams/data_stream/events_latest.jsonl"
    
    try:
        analyzer = StockSentimentAnalyzer()
        
        if args.mode == "crawl":
            events = analyzer.crawl_and_process(
                subreddit=args.subreddit,
                max_posts=args.limit,
                output_file=output_file,
                verbose=args.verbose
            )
        else:
            events = analyzer.fetch_and_process_posts(
                subreddit=args.subreddit,
                limit=args.limit,
                output_file=output_file,
                verbose=args.verbose
            )
        
        if args.print_json:
            for event in events:
                print(json.dumps(event.to_dict(), indent=2))
        
        if args.verbose:
            print(f"\n{'='*50}")
            print(f"Processed {len(events)} posts")
                
            tickers = {}
            sentiments = {"positive": 0, "neutral": 0, "negative": 0}
            
            for event in events:
                tickers[event.ticker] = tickers.get(event.ticker, 0) + 1
                sentiments[event.sentiment] = sentiments.get(event.sentiment, 0) + 1
            
            print(f"\nTop Tickers:")
            for ticker, count in sorted(tickers.items(), key=lambda x: -x[1])[:10]:
                print(f"  {ticker}: {count}")
            
            print(f"\nSentiment Distribution:")
            for sentiment, count in sentiments.items():
                pct = (count / len(events) * 100) if events else 0
                print(f"  {sentiment}: {count} ({pct:.1f}%)")
        
        analyzer.close()
        
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
