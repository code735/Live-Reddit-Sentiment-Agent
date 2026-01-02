import feedparser
from datetime import datetime
from typing import Dict, List
from bs4 import BeautifulSoup


def _clean_html(text: str) -> str:
    """Remove HTML tags from RSS summaries."""
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(strip=True)


def fetch_context_for_stock(stock: str) -> Dict[str, List]:
    """
    Fetches recent live headlines when Reddit sentiment is strong
    but context is missing.

    Source: Google News RSS (free, no API key)
    """

    FEED_URL = "https://news.google.com/rss/search?q={query}"

    feed = feedparser.parse(FEED_URL.format(query=stock))

    headlines: List[str] = []
    timestamps: List[str] = []
    source_credibility: List[float] = []
    short_summaries: List[str] = []
    links: List[str] = []

    for entry in feed.entries[:10]:  # limit noise
        # Headline
        headlines.append(entry.get("title", "Unknown headline"))

        # Article link
        links.append(entry.get("link"))

        # Timestamp (ISO 8601)
        try:
            published = datetime(*entry.published_parsed[:6]).isoformat()
        except Exception:
            published = None
        timestamps.append(published)

        # Source & credibility heuristic
        source = entry.get("source", {}).get("title", "").lower()
        credibility = 0.9 if any(
            s in source for s in ["Reuters", "Bloomberg", "wsj", "Financial Times", "The Economic Times"]
        ) else 0.7
        source_credibility.append(credibility)

        # Clean summary (or fallback)
        raw_summary = entry.get("summary", "")
        clean_summary = _clean_html(raw_summary)

        if not clean_summary:
            clean_summary = "No clear summary available from source."

        short_summaries.append(clean_summary)

    return {
        "headlines": headlines,
        "links": links,
        "timestamps": timestamps,
        "source_credibility": source_credibility,
        "short_summaries": short_summaries,
    }

def fetch_recent_news(ticker):
  data = fetch_context_for_stock(ticker)

  for i in range(len(data["headlines"])):
      print("\n")
      print(f"Article {i}: ")
      print("Headline:", data["headlines"][i])
      print("Link:", data["links"][i])
      print("Time:", data["timestamps"][i])
      print("Credibility:", data["source_credibility"][i])
      print("Summary:", data["short_summaries"][i])

  return data


fetch_recent_news("MAHKTECH")