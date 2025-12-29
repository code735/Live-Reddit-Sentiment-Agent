from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


class ContentType(Enum):
    POST = "post"
    COMMENT = "comment"


@dataclass
class Author:
    username: str
    is_deleted: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PostMedia:
    type: str  # "image", "video", "gallery", "link", "self"
    url: Optional[str] = None
    thumbnail: Optional[str] = None
    gallery_urls: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Comment:
    # Unique identifiers
    comment_id: str  # Reddit's comment ID (e.g., "abc123")
    post_id: str  # Parent post ID
    parent_id: str  # Parent comment ID or post ID
    
    # Content
    body: str  # Comment text (markdown)
    body_html: Optional[str] = None  # HTML version
    
    # Author info
    author: str = "[deleted]"
    author_is_deleted: bool = False
    
    # Metrics
    score: int = 0
    upvote_ratio: Optional[float] = None
    is_controversial: bool = False
    gilded: int = 0
    
    # Status flags
    is_submitter: bool = False  # Is this the post author
    is_edited: bool = False
    edited_at: Optional[datetime] = None
    is_deleted: bool = False
    is_removed: bool = False
    stickied: bool = False
    
    # Hierarchy
    depth: int = 0  # Comment depth in thread
    reply_count: int = 0
    
    # Timestamps
    created_utc: datetime = field(default_factory=datetime.utcnow)
    fetched_at: datetime = field(default_factory=datetime.utcnow)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    # For tracking changes
    content_hash: str = ""  # Hash of body + score for change detection
    
    # Subreddit info
    subreddit: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        for key in ['created_utc', 'fetched_at', 'last_updated', 'edited_at']:
            if data[key] and isinstance(data[key], datetime):
                data[key] = data[key]
        return data


@dataclass
class Post:
    # Unique identifiers
    post_id: str  # Reddit's post ID (e.g., "abc123")
    permalink: str  # Full permalink to post
    url: str  # URL the post links to (or self URL)
    
    # Content
    title: str
    selftext: str = ""  # Post body (for self posts)
    selftext_html: Optional[str] = None  # HTML version
    
    # Author info
    author: str = "[deleted]"
    author_is_deleted: bool = False
    
    # Subreddit info
    subreddit: str = ""
    subreddit_id: str = ""
    
    # Metrics
    score: int = 0
    upvote_ratio: float = 0.0
    num_comments: int = 0
    gilded: int = 0
    
    # Post type and media
    is_self: bool = True  # Self post vs link post
    is_video: bool = False
    is_gallery: bool = False
    media: Optional[PostMedia] = None
    
    # Link flair
    link_flair_text: Optional[str] = None
    link_flair_css_class: Optional[str] = None
    
    # Status flags
    is_edited: bool = False
    edited_at: Optional[datetime] = None
    is_deleted: bool = False
    is_removed: bool = False
    stickied: bool = False
    locked: bool = False
    spoiler: bool = False
    nsfw: bool = False
    
    # Timestamps
    created_utc: datetime = field(default_factory=datetime.utcnow)
    fetched_at: datetime = field(default_factory=datetime.utcnow)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    # For tracking changes
    content_hash: str = ""  # Hash of content for change detection
    
    # Comment tracking
    comment_ids: List[str] = field(default_factory=list)  # IDs of fetched comments
    comments_fetched_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        if data['media']:
            data['media'] = self.media.to_dict() if self.media else None
        return data


@dataclass
class CrawlState:
    subreddit: str
    last_post_id: Optional[str] = None
    after_token: Optional[str] = None  # Reddit's pagination token
    posts_crawled: int = 0
    comments_crawled: int = 0
    started_at: datetime = field(default_factory=datetime.utcnow)
    last_activity: datetime = field(default_factory=datetime.utcnow)
    is_complete: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

