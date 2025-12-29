import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging

from .models import Post, Comment, PostMedia

logger = logging.getLogger(__name__)


def compute_content_hash(content: str, score: int) -> str:
    data = f"{content}:{score}"
    return hashlib.md5(data.encode('utf-8')).hexdigest()


def parse_timestamp(timestamp: Optional[float]) -> Optional[datetime]:
    if timestamp is None or timestamp == 0:
        return None
    try:
        return datetime.utcfromtimestamp(timestamp)
    except (ValueError, OSError):
        return None


def safe_get(data: Dict[str, Any], *keys, default=None) -> Any:
    result = data
    for key in keys:
        if isinstance(result, dict):
            result = result.get(key, default)
        else:
            return default
    return result


class RedditParser:
    
    def __init__(self, include_deleted: bool = False):
        self.include_deleted = include_deleted
        self.posts_parsed = 0
        self.comments_parsed = 0
    
    def _is_deleted(self, author: str, body: Optional[str] = None) -> bool:
        if author in ("[deleted]", "[removed]"):
            return True
        if body and body in ("[deleted]", "[removed]"):
            return True
        return False
    
    def _parse_media(self, post_data: Dict[str, Any]) -> Optional[PostMedia]:
        try:
            is_self = post_data.get("is_self", True)
            is_video = post_data.get("is_video", False)
            is_gallery = post_data.get("is_gallery", False)
            
            if is_self:
                return PostMedia(type="self")
            
            if is_video:
                video_url = safe_get(post_data, "media", "reddit_video", "fallback_url")
                return PostMedia(
                    type="video",
                    url=video_url,
                    thumbnail=post_data.get("thumbnail")
                )
            
            if is_gallery:
                gallery_urls = []
                gallery_data = post_data.get("gallery_data", {})
                media_metadata = post_data.get("media_metadata", {})
                
                for item in gallery_data.get("items", []):
                    media_id = item.get("media_id")
                    if media_id and media_id in media_metadata:
                        media = media_metadata[media_id]
                        if "s" in media:
                            url = media["s"].get("u", "").replace("&amp;", "&")
                            if url:
                                gallery_urls.append(url)
                
                return PostMedia(
                    type="gallery",
                    gallery_urls=gallery_urls,
                    thumbnail=post_data.get("thumbnail")
                )
            
            url = post_data.get("url", "")
            post_hint = post_data.get("post_hint", "link")
            
            return PostMedia(
                type=post_hint,
                url=url,
                thumbnail=post_data.get("thumbnail")
            )
            
        except Exception as e:
            logger.warning(f"Error parsing media: {e}")
            return None
    
    def parse_post(self, post_data: Dict[str, Any]) -> Optional[Post]:
        try:
            author = post_data.get("author", "[deleted]")
            selftext = post_data.get("selftext", "")
            
            if self._is_deleted(author, selftext) and not self.include_deleted:
                return None
            
            edited = post_data.get("edited")
            edited_at = None
            is_edited = False
            if edited and edited is not False:
                is_edited = True
                edited_at = parse_timestamp(float(edited))
            
            post = Post(
                post_id=post_data.get("id", ""),
                permalink=post_data.get("permalink", ""),
                url=post_data.get("url", ""),
                title=post_data.get("title", ""),
                selftext=selftext,
                selftext_html=post_data.get("selftext_html"),
                author=author,
                author_is_deleted=self._is_deleted(author),
                subreddit=post_data.get("subreddit", ""),
                subreddit_id=post_data.get("subreddit_id", ""),
                score=post_data.get("score", 0),
                upvote_ratio=post_data.get("upvote_ratio", 0.0),
                num_comments=post_data.get("num_comments", 0),
                gilded=post_data.get("gilded", 0),
                is_self=post_data.get("is_self", True),
                is_video=post_data.get("is_video", False),
                is_gallery=post_data.get("is_gallery", False),
                media=self._parse_media(post_data),
                link_flair_text=post_data.get("link_flair_text"),
                link_flair_css_class=post_data.get("link_flair_css_class"),
                is_edited=is_edited,
                edited_at=edited_at,
                is_deleted=self._is_deleted(author),
                is_removed=author == "[removed]",
                stickied=post_data.get("stickied", False),
                locked=post_data.get("locked", False),
                spoiler=post_data.get("spoiler", False),
                nsfw=post_data.get("over_18", False),
                created_utc=parse_timestamp(post_data.get("created_utc")) or datetime.utcnow(),
                fetched_at=datetime.utcnow(),
                last_updated=datetime.utcnow(),
                content_hash=compute_content_hash(
                    f"{post_data.get('title', '')}:{selftext}",
                    post_data.get("score", 0)
                )
            )
            
            self.posts_parsed += 1
            return post
            
        except Exception as e:
            logger.error(f"Error parsing post: {e}")
            return None
    
    def parse_comment(
        self,
        comment_data: Dict[str, Any],
        post_id: str,
        subreddit: str,
        depth: int = 0
    ) -> Optional[Comment]:
        try:
            if comment_data.get("kind") == "more":
                return None
            
            author = comment_data.get("author", "[deleted]")
            body = comment_data.get("body", "")
            
            if self._is_deleted(author, body) and not self.include_deleted:
                return None
            
            parent_id = comment_data.get("parent_id", "")
            if parent_id.startswith("t1_"):
                parent_id = parent_id[3:]  
            elif parent_id.startswith("t3_"):
                parent_id = parent_id[3:]  
            
            edited = comment_data.get("edited")
            edited_at = None
            is_edited = False
            if edited and edited is not False:
                is_edited = True
                edited_at = parse_timestamp(float(edited))
            
            replies = comment_data.get("replies", "")
            reply_count = 0
            if isinstance(replies, dict):
                reply_children = safe_get(replies, "data", "children", default=[])
                reply_count = len(reply_children)
            
            comment = Comment(
                comment_id=comment_data.get("id", ""),
                post_id=post_id,
                parent_id=parent_id,
                body=body,
                body_html=comment_data.get("body_html"),
                author=author,
                author_is_deleted=self._is_deleted(author),
                score=comment_data.get("score", 0),
                is_controversial=comment_data.get("controversiality", 0) > 0,
                gilded=comment_data.get("gilded", 0),
                is_submitter=comment_data.get("is_submitter", False),
                is_edited=is_edited,
                edited_at=edited_at,
                is_deleted=self._is_deleted(author, body),
                is_removed=author == "[removed]" or body == "[removed]",
                stickied=comment_data.get("stickied", False),
                depth=depth,
                reply_count=reply_count,
                created_utc=parse_timestamp(comment_data.get("created_utc")) or datetime.utcnow(),
                fetched_at=datetime.utcnow(),
                last_updated=datetime.utcnow(),
                content_hash=compute_content_hash(body, comment_data.get("score", 0)),
                subreddit=subreddit
            )
            
            self.comments_parsed += 1
            return comment
            
        except Exception as e:
            logger.error(f"Error parsing comment: {e}")
            return None
    
    def parse_post_listing(
        self,
        response_data: Dict[str, Any]
    ) -> Tuple[List[Post], Optional[str]]:  
        posts = []
        after_token = None
        
        try:
            listing_data = response_data.get("data", {})
            after_token = listing_data.get("after")
            children = listing_data.get("children", [])
            
            for child in children:
                if child.get("kind") == "t3":  
                    post = self.parse_post(child.get("data", {}))
                    if post:
                        posts.append(post)
            
            logger.info(f"Parsed {len(posts)} posts from listing")
            
        except Exception as e:
            logger.error(f"Error parsing post listing: {e}")
        
        return posts, after_token
    
    def parse_comments_page(
        self,
        response_data: List[Dict[str, Any]],
        post_id: str,
        subreddit: str,
        max_depth: int = 10
    ) -> Tuple[Optional[Post], List[Comment]]:
        post = None
        comments = []
        
        try:
            if not response_data or len(response_data) < 2:
                logger.warning("Invalid comment page response format")
                return None, []
            
            post_listing = response_data[0]
            post_children = safe_get(post_listing, "data", "children", default=[])
            if post_children:
                post = self.parse_post(post_children[0].get("data", {}))
            
            comment_listing = response_data[1]
            comment_children = safe_get(comment_listing, "data", "children", default=[])
            
            comments = self._parse_comment_tree(
                comment_children, post_id, subreddit, 0, max_depth
            )
            
            logger.info(f"Parsed {len(comments)} comments for post {post_id}")
            
        except Exception as e:
            logger.error(f"Error parsing comment page: {e}")
        
        return post, comments
    
    def _parse_comment_tree(
        self,
        children: List[Dict[str, Any]],
        post_id: str,
        subreddit: str,
        current_depth: int,
        max_depth: int
    ) -> List[Comment]:
        comments = []
        
        if current_depth > max_depth:
            return comments
        
        for child in children:
            kind = child.get("kind")
            
            if kind == "more":
                continue
            
            if kind == "t1":  
                data = child.get("data", {})
                comment = self.parse_comment(data, post_id, subreddit, current_depth)
                
                if comment:
                    comments.append(comment)
                    
                    replies = data.get("replies")
                    if isinstance(replies, dict):
                        reply_children = safe_get(replies, "data", "children", default=[])
                        nested_comments = self._parse_comment_tree(
                            reply_children, post_id, subreddit, 
                            current_depth + 1, max_depth
                        )
                        comments.extend(nested_comments)
        
        return comments
    
    def get_stats(self) -> Dict[str, int]:
        return {
            "posts_parsed": self.posts_parsed,
            "comments_parsed": self.comments_parsed
        }
    
    def reset_stats(self):
        self.posts_parsed = 0
        self.comments_parsed = 0

