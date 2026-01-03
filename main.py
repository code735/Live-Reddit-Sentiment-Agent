import os
import re
import json
import uuid
import time
import hashlib
import argparse
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, asdict

from dotenv import load_dotenv
from google import genai
from google.genai import types

from config import config
from crawler.crawler import RedditCrawler
from crawler.database import RedditDatabase
from sentiment.app.services.sentiment import SentimentService


# Indian stock ticker mappings for regex-based extraction
INDIAN_STOCK_MAPPINGS = {
    # Tata Group
    r"\b(tcs|tata\s*consultancy)\b": "TCS",
    r"\b(tata\s*motors?|tatamotors?)\b": "TATAMOTORS",
    r"\b(tata\s*steel|tatasteel)\b": "TATASTEEL",
    r"\b(tata\s*power|tatapower)\b": "TATAPOWER",
    r"\b(tata\s*elxsi|tataelxsi)\b": "TATAELXSI",
    r"\b(trent)\b": "TRENT",
    # Reliance
    r"\b(reliance|ril)\b": "RELIANCE",
    r"\b(jio\s*fin|jiofin)\b": "JIOFIN",
    # IT Companies
    r"\b(infosys|infy)\b": "INFY",
    r"\b(wipro)\b": "WIPRO",
    r"\b(hcl\s*tech|hcltech)\b": "HCLTECH",
    r"\b(tech\s*mahindra|techm)\b": "TECHM",
    # Banks
    r"\b(hdfc\s*bank|hdfcbank)\b": "HDFCBANK",
    r"\b(icici\s*bank|icicibank)\b": "ICICIBANK",
    r"\b(sbi|state\s*bank)\b": "SBIN",
    r"\b(axis\s*bank|axisbank)\b": "AXISBANK",
    r"\b(kotak|kotakbank)\b": "KOTAKBANK",
    r"\b(indusind|indusindbk)\b": "INDUSINDBK",
    r"\b(yes\s*bank|yesbank)\b": "YESBANK",
    r"\b(bank\s*of\s*baroda|bob|bankbaroda)\b": "BANKBARODA",
    r"\b(pnb|punjab\s*national)\b": "PNB",
    # Telecom
    r"\b(airtel|bharti\s*airtel|bhartiartl)\b": "BHARTIARTL",
    # Consumer/FMCG
    r"\b(itc)\b": "ITC",
    r"\b(hul|hindustan\s*unilever|hindunilvr)\b": "HINDUNILVR",
    r"\b(nestle|nestleind)\b": "NESTLEIND",
    r"\b(britannia)\b": "BRITANNIA",
    r"\b(asian\s*paints?|asianpaint)\b": "ASIANPAINT",
    r"\b(pidilite|pidilitind)\b": "PIDILITIND",
    r"\b(titan)\b": "TITAN",
    r"\b(dmart|avenue\s*supermarts?)\b": "DMART",
    # Auto
    r"\b(maruti|maruti\s*suzuki)\b": "MARUTI",
    r"\b(m&m|mahindra)\b": "M&M",
    r"\b(bajaj\s*auto)\b": "BAJAJ-AUTO",
    r"\b(hero\s*motocorp|heromotoco)\b": "HEROMOTOCO",
    r"\b(tvs\s*motor|tvsmotor)\b": "TVSMOTOR",
    r"\b(eicher|eichermot)\b": "EICHERMOT",
    # Finance
    r"\b(bajaj\s*finance|bajfinance)\b": "BAJFINANCE",
    r"\b(bajaj\s*finserv|bajajfinsv)\b": "BAJAJFINSV",
    r"\b(hdfc\s*life|hdfclife)\b": "HDFCLIFE",
    r"\b(icici\s*pru|icicipruli)\b": "ICICIPRULI",
    # Pharma
    r"\b(sun\s*pharma|sunpharma)\b": "SUNPHARMA",
    r"\b(dr\s*reddy|drreddy)\b": "DRREDDY",
    r"\b(cipla)\b": "CIPLA",
    r"\b(zydus|zyduslife)\b": "ZYDUSLIFE",
    # Adani
    r"\b(adani\s*ent|adanient)\b": "ADANIENT",
    r"\b(adani\s*ports?|adaniports)\b": "ADANIPORTS",
    r"\b(adani\s*green|adanigreen)\b": "ADANIGREEN",
    r"\b(adani\s*power|adanipower)\b": "ADANIPOWER",
    # Infra/Energy
    r"\b(l&t|larsen)\b": "LT",
    r"\b(power\s*grid|powergrid)\b": "POWERGRID",
    r"\b(ntpc)\b": "NTPC",
    r"\b(coal\s*india|coalindia)\b": "COALINDIA",
    r"\b(ongc)\b": "ONGC",
    r"\b(ioc|indian\s*oil)\b": "IOC",
    r"\b(bpcl)\b": "BPCL",
    # Metals
    r"\b(vedanta|vedl)\b": "VEDL",
    r"\b(jsw\s*steel|jswsteel)\b": "JSWSTEEL",
    r"\b(hindalco)\b": "HINDALCO",
    r"\b(ultratech|ultracemco)\b": "ULTRACEMCO",
    r"\b(grasim)\b": "GRASIM",
    # New Age
    r"\b(zomato)\b": "ZOMATO",
    r"\b(paytm|one97)\b": "PAYTM",
    r"\b(nykaa)\b": "NYKAA",
    r"\b(delhivery)\b": "DELHIVERY",
    r"\b(policybazaar|policybzr)\b": "POLICYBZR",
    # Electronics
    r"\b(havells)\b": "HAVELLS",
    r"\b(dixon)\b": "DIXON",
    r"\b(varun\s*beverages?|vbl)\b": "VBL",
    # US Stocks
    r"\b(tesla|tsla)\b": "TSLA",
    r"\b(apple|aapl)\b": "AAPL",
    r"\b(microsoft|msft)\b": "MSFT",
    r"\b(amazon|amzn)\b": "AMZN",
    r"\b(google|googl|alphabet)\b": "GOOGL",
    r"\b(meta|facebook)\b": "META",
    r"\b(nvidia|nvda)\b": "NVDA",
    r"\b(netflix|nflx)\b": "NFLX",
    r"\b(amd)\b": "AMD",
}


# System prompt for stock ticker extraction (optimized for Indian & US stocks)
TICKER_EXTRACTION_PROMPT = """You are a financial text analysis expert specializing in Indian stock markets. Your task is to identify stock tickers mentioned in Reddit posts and comments.

Given a text about stocks/trading, extract the stock ticker symbol(s) being discussed.

INDIAN STOCK MAPPINGS (NSE/BSE):
- Tata Consultancy Services / TCS = TCS
- Tata Motors = TATAMOTORS
- Tata Steel = TATASTEEL
- Tata Power = TATAPOWER
- Tata Consumer = TATACONSUM
- Tata Elxsi = TATAELXSI
- Reliance / RIL / Reliance Industries = RELIANCE
- Infosys / Infy = INFY
- HDFC Bank = HDFCBANK
- HDFC Life = HDFCLIFE
- HDFC AMC = HDFCAMC
- ICICI Bank = ICICIBANK
- ICICI Prudential = ICICIPRULI
- State Bank / SBI = SBIN
- Axis Bank = AXISBANK
- Kotak Mahindra Bank / Kotak = KOTAKBANK
- Bharti Airtel / Airtel = BHARTIARTL
- Jio Financial = JIOFIN
- Wipro = WIPRO
- HCL Tech / HCL Technologies = HCLTECH
- Tech Mahindra = TECHM
- L&T / Larsen & Toubro = LT
- ITC = ITC
- Asian Paints = ASIANPAINT
- Maruti Suzuki / Maruti = MARUTI
- Mahindra & Mahindra / M&M = M&M
- Bajaj Finance = BAJFINANCE
- Bajaj Finserv = BAJAJFINSV
- Bajaj Auto = BAJAJ-AUTO
- Hindustan Unilever / HUL = HINDUNILVR
- Nestle India = NESTLEIND
- Sun Pharma = SUNPHARMA
- Dr Reddy's = DRREDDY
- Cipla = CIPLA
- Adani Enterprises = ADANIENT
- Adani Ports = ADANIPORTS
- Adani Green = ADANIGREEN
- Adani Power = ADANIPOWER
- Power Grid = POWERGRID
- NTPC = NTPC
- Coal India = COALINDIA
- ONGC = ONGC
- IOC / Indian Oil = IOC
- BPCL = BPCL
- Zomato = ZOMATO
- Paytm / One97 = PAYTM
- Nykaa / FSN E-Commerce = NYKAA
- Delhivery = DELHIVERY
- Policybazaar / PB Fintech = POLICYBZR
- IndusInd Bank = INDUSINDBK
- Yes Bank = YESBANK
- Bank of Baroda / BoB = BANKBARODA
- Punjab National Bank / PNB = PNB
- Vedanta = VEDL
- JSW Steel = JSWSTEEL
- Hindalco = HINDALCO
- UltraTech Cement = ULTRACEMCO
- Grasim = GRASIM
- Titan = TITAN
- Avenue Supermarts / DMart = DMART
- Britannia = BRITANNIA
- Pidilite = PIDILITIND
- Havells = HAVELLS
- Dixon Technologies = DIXON
- Varun Beverages = VBL
- Trent = TRENT
- Zydus Lifesciences = ZYDUSLIFE
- Eicher Motors = EICHERMOT
- Hero MotoCorp = HEROMOTOCO
- TVS Motor = TVSMOTOR


Rules:
1. Return ONLY valid stock ticker symbols as used on NSE/BSE (Indian) or NYSE/NASDAQ (US)
2. If the text mentions company names, nicknames, or abbreviations, convert to official ticker
3. If multiple tickers are mentioned, return the PRIMARY one being discussed
4. If no clear stock ticker is found, return "UNKNOWN"
5. Return ONLY the ticker symbol, nothing else (e.g., "RELIANCE" not "The ticker is RELIANCE")
6. For Indian stocks, prefer NSE ticker format

Examples:
- "Reliance is looking strong after AGM" -> RELIANCE
- "TCS results were amazing, bullish on IT sector" -> TCS
- "Tata Motors EV play is exciting" -> TATAMOTORS  
- "HDFC Bank merger with HDFC Ltd complete" -> HDFCBANK
- "Infosys guidance was weak" -> INFY
- "Adani stocks recovering after Hindenburg" -> ADANIENT
- "Nifty IT index down, Wipro and HCL weak" -> WIPRO
- "Zomato Blinkit growth is insane" -> ZOMATO
- "TSLA to the moon! 🚀" -> TSLA
- "The market is crazy today" -> UNKNOWN
- "Nifty hitting all time high" -> UNKNOWN
- "FII selling continues" -> UNKNOWN
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

    def __init__(self, gemini_api_key: Optional[str] = None, use_llm: bool = True):
        self.api_key = gemini_api_key or os.environ.get("GEMINI_API_KEY")
        self.use_llm = use_llm

        if self.use_llm and not self.api_key:
            print(
                "Warning: GEMINI_API_KEY not set. Using regex-only ticker extraction."
            )
            self.use_llm = False

        if self.use_llm:
            self.gemini_client = genai.Client(api_key=self.api_key)
        else:
            self.gemini_client = None

        self.sentiment_service = SentimentService()
        self.crawler = RedditCrawler(config)
        self.last_llm_call = 0
        self.llm_rate_limit_delay = (
            15  # seconds between LLM calls (5 req/min = 12s, add buffer)
        )

        # Connect to database
        if not self.crawler.connect_database():
            raise ConnectionError("Failed to connect to database")

    def extract_ticker_regex(self, text: str) -> Optional[str]:
        """Fast regex-based ticker extraction for common Indian and US stocks."""
        text_lower = text.lower()

        for pattern, ticker in INDIAN_STOCK_MAPPINGS.items():
            if re.search(pattern, text_lower, re.IGNORECASE):
                return ticker

        # Also check for direct ticker mentions (uppercase, 2-12 chars for Indian tickers)
        direct_tickers = re.findall(r"\b([A-Z]{2,12}(?:-[A-Z]+)?)\b", text)
        # Filter for known NSE/BSE format tickers
        known_tickers = {
            "TCS",
            "INFY",
            "RELIANCE",
            "HDFCBANK",
            "ICICIBANK",
            "SBIN",
            "WIPRO",
            "ITC",
            "BHARTIARTL",
            "KOTAKBANK",
            "AXISBANK",
            "MARUTI",
            "TITAN",
            "BAJFINANCE",
            "HCLTECH",
            "TECHM",
            "SUNPHARMA",
            "HINDUNILVR",
            "LT",
            "ASIANPAINT",
            "NTPC",
            "POWERGRID",
            "ONGC",
            "COALINDIA",
            "TATASTEEL",
            "TATAMOTORS",
            "ADANIENT",
            "ADANIPORTS",
            "ZOMATO",
            "PAYTM",
            "NYKAA",
            "TSLA",
            "AAPL",
            "MSFT",
            "AMZN",
            "GOOGL",
            "META",
            "NVDA",
            "NFLX",
            "AMD",
        }

        for ticker in direct_tickers:
            if ticker in known_tickers:
                return ticker

        return None

    def extract_ticker_llm(self, text: str) -> str:
        """Use Gemini LLM to extract stock ticker (with rate limiting)."""
        if not self.use_llm or not self.gemini_client:
            return "UNKNOWN"

        # Rate limiting
        time_since_last = time.time() - self.last_llm_call
        if time_since_last < self.llm_rate_limit_delay:
            time.sleep(self.llm_rate_limit_delay - time_since_last)

        try:
            messages = [
                types.Content(
                    role="user",
                    parts=[
                        types.Part(
                            text=f"Extract the stock ticker from this text:\n\n{text[:2000]}"
                        )
                    ],
                )
            ]

            response = self.gemini_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=messages,
                config=types.GenerateContentConfig(
                    system_instruction=TICKER_EXTRACTION_PROMPT,
                    temperature=0.1,
                    max_output_tokens=20,
                ),
            )

            self.last_llm_call = time.time()

            if response and response.text:
                ticker = response.text.strip().upper()
                # Clean up - remove any extra text, handle hyphenated tickers
                ticker = ticker.split()[0] if ticker else "UNKNOWN"
                ticker = re.sub(r"[^A-Z0-9\-&]", "", ticker)
                # Validate it looks like a ticker
                if ticker and 1 <= len(ticker) <= 15 and ticker != "UNKNOWN":
                    return ticker

            return "UNKNOWN"

        except Exception as e:
            print(f"Error extracting ticker via LLM: {e}")
            return "UNKNOWN"

    def extract_ticker(self, text: str) -> str:
        """Extract stock ticker - tries regex first, then LLM as fallback."""
        # First try fast regex extraction
        ticker = self.extract_ticker_regex(text)
        if ticker:
            return ticker

        # Fall back to LLM for complex cases
        if self.use_llm:
            return self.extract_ticker_llm(text)

        return "UNKNOWN"

    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        try:
            if not text or len(text.strip()) < 5:
                return {"label": "neutral", "confidence": 0.5}

            result = self.sentiment_service.analyze(
                text[:512]
            )  # FinBERT has token limits
            return {
                "label": result["label"].lower(),
                "confidence": round(result["confidence"], 4),
            }
        except Exception as e:
            print(f"Error analyzing sentiment: {e}")
            return {"label": "neutral", "confidence": 0.5}

    def generate_event_id(self, post_id: str, observed_at: str) -> str:
        unique_string = f"{post_id}_{observed_at}"
        return hashlib.md5(unique_string.encode()).hexdigest()

    def process_post(
        self, post: Dict[str, Any], comments: List[Dict[str, Any]]
    ) -> StockEvent:
        observed_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        # Combine title and selftext for ticker extraction
        full_text = f"{post.get('title', '')} {post.get('selftext', '')}"

        # Extract ticker using Gemini
        ticker = self.extract_ticker(full_text)

        # Analyze sentiment using FinBERT
        sentiment_result = self.analyze_sentiment(full_text)

        # Process comments
        comment_list = []
        for comment in comments[:20]:  # Limit to first 20 comments
            if comment.get("author") and comment.get("body"):
                comment_list.append(
                    {
                        "user_name": comment.get("author", "[deleted]"),
                        "text": comment.get("body", "")[:500],  # Limit comment length
                    }
                )

        # Generate event ID
        event_id = self.generate_event_id(post["post_id"], observed_at)

        # Format created_at timestamp
        created_at = post.get("created_utc")
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat() + "Z"
        elif created_at is None:
            created_at = observed_at
        else:
            created_at = str(created_at)

        # Construct URL
        permalink = post.get("permalink", "")
        url = f"https://old.reddit.com{permalink}" if permalink else ""

        return StockEvent(
            event_id=event_id,
            post_id=post["post_id"],
            event_type="upsert",
            ticker=ticker,
            text=post.get("title", ""),
            post_des=post.get("selftext", "") or "",
            comments=comment_list,
            sentiment=sentiment_result["label"],
            confidence=sentiment_result["confidence"],
            source="reddit",
            created_at=created_at,
            observed_at=observed_at,
            url=url,
            author=post.get("author", "[deleted]"),
        )

    def fetch_and_process_posts(
        self,
        subreddit: Optional[str] = None,
        limit: int = 30,
        output_file: Optional[str] = None,
        verbose: bool = False,
    ) -> List[StockEvent]:
        subreddit = subreddit or config.subreddit

        if verbose:
            print(f"Fetching posts from r/{subreddit}...")

        # Get posts from database
        posts = self.crawler.database.get_posts(
            subreddit=subreddit, limit=limit, sort_by="created_utc", sort_order="DESC"
        )

        if verbose:
            print(f"Found {len(posts)} posts to process")

        events = []

        for i, post in enumerate(posts):
            if verbose:
                print(
                    f"Processing post {i+1}/{len(posts)}: {post.get('title', '')[:50]}..."
                )

            # Get comments for this post
            comments = self.crawler.database.get_comments_for_post(
                post_id=post["post_id"], limit=config.max_comments_per_post
            )

            # Process the post
            event = self.process_post(post, comments)
            events.append(event)

            if verbose:
                print(
                    f"  Ticker: {event.ticker}, Sentiment: {event.sentiment} ({event.confidence:.2%})"
                )

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
        verbose: bool = False,
    ) -> List[StockEvent]:
        subreddit = subreddit or config.subreddit

        if verbose:
            print(f"Crawling r/{subreddit} for new posts...")

        # Crawl new posts
        result = self.crawler.crawl(
            subreddit=subreddit, max_posts=max_posts, fetch_comments=True, resume=False
        )

        if verbose:
            print(
                f"Crawl complete: {result.posts_fetched} posts, {result.comments_fetched} comments"
            )
            if result.errors:
                print(f"Errors: {result.errors}")

        # Now process the posts from database
        return self.fetch_and_process_posts(
            subreddit=subreddit,
            limit=max_posts,
            output_file=output_file,
            verbose=verbose,
        )

    def write_events_to_jsonl(self, events: List[StockEvent], output_file: str):
        with open(output_file, "a", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event.to_dict(), ensure_ascii=False) + "\n")

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
        help="Mode: 'fetch' to process existing posts from DB, 'crawl' to fetch new posts first",
    )
    parser.add_argument(
        "--subreddit",
        type=str,
        default=None,
        help=f"Subreddit to analyze (default: {config.subreddit})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="Maximum number of posts to process (default: 30)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSONL file path (default: pathway_streams/data_stream/events_latest.jsonl)",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument(
        "--print-json", action="store_true", help="Print events as JSON to stdout"
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable LLM for ticker extraction (use regex only - faster, no rate limits)",
    )

    args = parser.parse_args()

    output_file = args.output or "pathway_streams/data_stream/events_latest.jsonl"

    try:
        analyzer = StockSentimentAnalyzer(use_llm=not args.no_llm)

        if args.mode == "crawl":
            events = analyzer.crawl_and_process(
                subreddit=args.subreddit,
                max_posts=args.limit,
                output_file=output_file,
                verbose=args.verbose,
            )
        else:
            events = analyzer.fetch_and_process_posts(
                subreddit=args.subreddit,
                limit=args.limit,
                output_file=output_file,
                verbose=args.verbose,
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
