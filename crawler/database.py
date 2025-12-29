from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging

from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, ConnectionFailure

from .models import Post, Comment, CrawlState
from .config import CrawlerConfig

logger = logging.getLogger(__name__)


class RedditDatabase:
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        
        # Collection references
        self._posts: Optional[Collection] = None
        self._comments: Optional[Collection] = None
        self._crawl_states: Optional[Collection] = None
        self._change_log: Optional[Collection] = None
        
        # Statistics
        self.stats = {
            "posts_inserted": 0,
            "posts_updated": 0,
            "comments_inserted": 0,
            "comments_updated": 0,
            "changes_logged": 0
        }
    
    def connect(self) -> bool:
        try:
            self.client = MongoClient(
                self.config.mongo_uri,
                serverSelectionTimeoutMS=5000
            )
            
            self.client.admin.command('ping')
            
            self.db = self.client[self.config.database_name]
            
            self._posts = self.db["posts"]
            self._comments = self.db["comments"]
            self._crawl_states = self.db["crawl_states"]
            self._change_log = self.db["change_log"]
            
            self._create_indexes()
            
            logger.info(f"Connected to MongoDB: {self.config.database_name}")
            return True
            
        except ConnectionFailure as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")
            return False
    
    def _create_indexes(self):
        
        self._posts.create_index([("post_id", ASCENDING)], unique=True)
        self._posts.create_index([("subreddit", ASCENDING)])
        self._posts.create_index([("created_utc", DESCENDING)])
        self._posts.create_index([("score", DESCENDING)])
        self._posts.create_index([("author", ASCENDING)])
        self._posts.create_index([("link_flair_text", ASCENDING)])
        self._posts.create_index([("fetched_at", DESCENDING)])
        
        self._posts.create_index([
            ("title", TEXT),
            ("selftext", TEXT)
        ], name="post_text_search")
        
        self._posts.create_index([
            ("subreddit", ASCENDING),
            ("created_utc", DESCENDING)
        ])
        self._posts.create_index([
            ("subreddit", ASCENDING),
            ("score", DESCENDING)
        ])
        
        self._comments.create_index([("comment_id", ASCENDING)], unique=True)
        self._comments.create_index([("post_id", ASCENDING)])
        self._comments.create_index([("parent_id", ASCENDING)])
        self._comments.create_index([("author", ASCENDING)])
        self._comments.create_index([("subreddit", ASCENDING)])
        self._comments.create_index([("created_utc", DESCENDING)])
        self._comments.create_index([("score", DESCENDING)])
        self._comments.create_index([("depth", ASCENDING)])
        
        self._comments.create_index([("body", TEXT)], name="comment_text_search")
        
        self._comments.create_index([
            ("post_id", ASCENDING),
            ("created_utc", ASCENDING)
        ])
        self._comments.create_index([
            ("post_id", ASCENDING),
            ("depth", ASCENDING)
        ])
        
        self._crawl_states.create_index([("subreddit", ASCENDING)], unique=True)
        
        self._change_log.create_index([("content_type", ASCENDING)])
        self._change_log.create_index([("content_id", ASCENDING)])
        self._change_log.create_index([("changed_at", DESCENDING)])
        self._change_log.create_index([("change_type", ASCENDING)])
        
        logger.info("Database indexes created")
    
    def close(self):
        if self.client:
            self.client.close()
            logger.info("Database connection closed")
    
    def upsert_post(self, post: Post) -> Tuple[bool, str]:
        try:
            post_dict = post.to_dict()
            existing = self._posts.find_one({"post_id": post.post_id})
            
            if existing:
                if existing.get("content_hash") != post.content_hash:
                    self._log_change(
                        content_type="post",
                        content_id=post.post_id,
                        change_type="content_updated",
                        old_value={
                            "content_hash": existing.get("content_hash"),
                            "score": existing.get("score"),
                            "num_comments": existing.get("num_comments"),
                            "selftext": existing.get("selftext", "")[:500]
                        },
                        new_value={
                            "content_hash": post.content_hash,
                            "score": post.score,
                            "num_comments": post.num_comments,
                            "selftext": post.selftext[:500]
                        }
                    )
                    
                    post_dict["first_seen"] = existing.get("first_seen", existing.get("fetched_at"))
                    self._posts.update_one(
                        {"post_id": post.post_id},
                        {"$set": post_dict}
                    )
                    self.stats["posts_updated"] += 1
                    return True, "updated"
                else:
                    self._posts.update_one(
                        {"post_id": post.post_id},
                        {"$set": {"last_updated": datetime.utcnow()}}
                    )
                    return False, "unchanged"
            else:
                post_dict["first_seen"] = datetime.utcnow()
                self._posts.insert_one(post_dict)
                self.stats["posts_inserted"] += 1
                return True, "inserted"
                
        except DuplicateKeyError:
            logger.warning(f"Duplicate post ID: {post.post_id}")
            return False, "duplicate"
        except Exception as e:
            logger.error(f"Error upserting post: {e}")
            return False, "error"
    
    def get_post(self, post_id: str) -> Optional[Dict[str, Any]]:
        return self._posts.find_one({"post_id": post_id})
    
    def get_posts(
        self,
        subreddit: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
        sort_by: str = "created_utc",
        sort_order: int = DESCENDING,
        min_score: Optional[int] = None,
        flair: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        query = {}
        
        if subreddit:
            query["subreddit"] = subreddit
        if min_score is not None:
            query["score"] = {"$gte": min_score}
        if flair:
            query["link_flair_text"] = flair
        if since:
            query["created_utc"] = {"$gte": since}
        if until:
            if "created_utc" in query:
                query["created_utc"]["$lte"] = until
            else:
                query["created_utc"] = {"$lte": until}
        
        cursor = self._posts.find(query).sort(sort_by, sort_order).skip(skip).limit(limit)
        return list(cursor)
    
    def search_posts(self, text_query: str, limit: int = 100) -> List[Dict[str, Any]]:
        cursor = self._posts.find(
            {"$text": {"$search": text_query}},
            {"score": {"$meta": "textScore"}}
        ).sort([("score", {"$meta": "textScore"})]).limit(limit)
        return list(cursor)
    
    def upsert_comment(self, comment: Comment) -> Tuple[bool, str]:
        try:
            comment_dict = comment.to_dict()
            existing = self._comments.find_one({"comment_id": comment.comment_id})
            
            if existing:
                if existing.get("content_hash") != comment.content_hash:
                    self._log_change(
                        content_type="comment",
                        content_id=comment.comment_id,
                        change_type="content_updated",
                        old_value={
                            "content_hash": existing.get("content_hash"),
                            "score": existing.get("score"),
                            "body": existing.get("body", "")[:500]
                        },
                        new_value={
                            "content_hash": comment.content_hash,
                            "score": comment.score,
                            "body": comment.body[:500]
                        }
                    )
                    
                    comment_dict["first_seen"] = existing.get("first_seen", existing.get("fetched_at"))
                    self._comments.update_one(
                        {"comment_id": comment.comment_id},
                        {"$set": comment_dict}
                    )
                    self.stats["comments_updated"] += 1
                    return True, "updated"
                else:
                    self._comments.update_one(
                        {"comment_id": comment.comment_id},
                        {"$set": {"last_updated": datetime.utcnow()}}
                    )
                    return False, "unchanged"
            else:
                comment_dict["first_seen"] = datetime.utcnow()
                self._comments.insert_one(comment_dict)
                self.stats["comments_inserted"] += 1
                
                self._posts.update_one(
                    {"post_id": comment.post_id},
                    {
                        "$addToSet": {"comment_ids": comment.comment_id},
                        "$set": {"comments_fetched_at": datetime.utcnow()}
                    }
                )
                return True, "inserted"
                
        except DuplicateKeyError:
            logger.warning(f"Duplicate comment ID: {comment.comment_id}")
            return False, "duplicate"
        except Exception as e:
            logger.error(f"Error upserting comment: {e}")
            return False, "error"
    
    def get_comment(self, comment_id: str) -> Optional[Dict[str, Any]]:
        return self._comments.find_one({"comment_id": comment_id})
    
    def get_comments_for_post(
        self,
        post_id: str,
        limit: int = 1000,
        sort_by: str = "created_utc",
        sort_order: int = ASCENDING
    ) -> List[Dict[str, Any]]:
        cursor = self._comments.find(
            {"post_id": post_id}
        ).sort(sort_by, sort_order).limit(limit)
        return list(cursor)
    
    def get_comment_thread(self, parent_id: str) -> List[Dict[str, Any]]:
        return list(self._comments.find({"parent_id": parent_id}))
    
    def get_comments(
        self,
        subreddit: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
        min_score: Optional[int] = None,
        max_depth: Optional[int] = None,
        since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        query = {}
        
        if subreddit:
            query["subreddit"] = subreddit
        if min_score is not None:
            query["score"] = {"$gte": min_score}
        if max_depth is not None:
            query["depth"] = {"$lte": max_depth}
        if since:
            query["created_utc"] = {"$gte": since}
        
        cursor = self._comments.find(query).sort("created_utc", DESCENDING).skip(skip).limit(limit)
        return list(cursor)
    
    def search_comments(self, text_query: str, limit: int = 100) -> List[Dict[str, Any]]:
        cursor = self._comments.find(
            {"$text": {"$search": text_query}},
            {"score": {"$meta": "textScore"}}
        ).sort([("score", {"$meta": "textScore"})]).limit(limit)
        return list(cursor)
    
    def _log_change(
        self,
        content_type: str,
        content_id: str,
        change_type: str,
        old_value: Dict[str, Any],
        new_value: Dict[str, Any]
    ):
        try:
            self._change_log.insert_one({
                "content_type": content_type,
                "content_id": content_id,
                "change_type": change_type,
                "old_value": old_value,
                "new_value": new_value,
                "changed_at": datetime.utcnow()
            })
            self.stats["changes_logged"] += 1
        except Exception as e:
            logger.error(f"Error logging change: {e}")
    
    def get_changes(
        self,
        content_type: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        query = {}
        if content_type:
            query["content_type"] = content_type
        if since:
            query["changed_at"] = {"$gte": since}
        
        cursor = self._change_log.find(query).sort("changed_at", DESCENDING).limit(limit)
        return list(cursor)
    
    def get_crawl_state(self, subreddit: str) -> Optional[CrawlState]:
        doc = self._crawl_states.find_one({"subreddit": subreddit})
        if doc:
            return CrawlState(
                subreddit=doc["subreddit"],
                last_post_id=doc.get("last_post_id"),
                after_token=doc.get("after_token"),
                posts_crawled=doc.get("posts_crawled", 0),
                comments_crawled=doc.get("comments_crawled", 0),
                started_at=doc.get("started_at", datetime.utcnow()),
                last_activity=doc.get("last_activity", datetime.utcnow()),
                is_complete=doc.get("is_complete", False)
            )
        return None
    
    def save_crawl_state(self, state: CrawlState):
        self._crawl_states.update_one(
            {"subreddit": state.subreddit},
            {"$set": state.to_dict()},
            upsert=True
        )
    
    def reset_crawl_state(self, subreddit: str):        
        self._crawl_states.delete_one({"subreddit": subreddit})
    
    def export_for_ml(
        self,
        subreddit: Optional[str] = None,
        include_comments: bool = True,
        flatten: bool = False
    ) -> List[Dict[str, Any]]:
        query = {"subreddit": subreddit} if subreddit else {}
        posts = list(self._posts.find(query))
        
        if not include_comments:
            return posts
        
        if flatten:
            comments = list(self._comments.find(query))
            return posts + comments
        
        for post in posts:
            post["comments"] = list(
                self._comments.find({"post_id": post["post_id"]})
            )
        
        return posts
    
    def get_stats(self) -> Dict[str, Any]:
        return {
            **self.stats,
            "total_posts": self._posts.count_documents({}),
            "total_comments": self._comments.count_documents({}),
            "total_changes": self._change_log.count_documents({})
        }
    
    def reset_stats(self):
        self.stats = {
            "posts_inserted": 0,
            "posts_updated": 0,
            "comments_inserted": 0,
            "comments_updated": 0,
            "changes_logged": 0
        }

