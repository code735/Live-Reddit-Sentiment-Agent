from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import json
import logging

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_values

from .models import Post, Comment, CrawlState
from .config import CrawlerConfig

logger = logging.getLogger(__name__)


class RedditDatabase:
    
    def __init__(self, config: CrawlerConfig):
        self.config = config
        self.conn: Optional[psycopg2.extensions.connection] = None
        
        self.stats = {
            "posts_inserted": 0,
            "posts_updated": 0,
            "comments_inserted": 0,
            "comments_updated": 0,
            "changes_logged": 0
        }
    
    def connect(self) -> bool:
        try:
            self.conn = psycopg2.connect(
                host=self.config.postgres_host,
                port=self.config.postgres_port,
                user=self.config.postgres_user,
                password=self.config.postgres_password,
                dbname=self.config.postgres_database
            )
            self.conn.autocommit = False
            
            self._create_tables()
            
            logger.info(f"Connected to PostgreSQL: {self.config.postgres_database}")
            return True
            
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
        except Exception as e:
            logger.error(f"PostgreSQL connection error: {e}")
            return False
    
    def _create_tables(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id SERIAL PRIMARY KEY,
                    post_id VARCHAR(20) UNIQUE NOT NULL,
                    permalink TEXT,
                    url TEXT,
                    title TEXT NOT NULL,
                    selftext TEXT,
                    selftext_html TEXT,
                    author VARCHAR(100),
                    author_is_deleted BOOLEAN DEFAULT FALSE,
                    subreddit VARCHAR(100),
                    subreddit_id VARCHAR(20),
                    score INTEGER DEFAULT 0,
                    upvote_ratio FLOAT DEFAULT 0.0,
                    num_comments INTEGER DEFAULT 0,
                    gilded INTEGER DEFAULT 0,
                    is_self BOOLEAN DEFAULT TRUE,
                    is_video BOOLEAN DEFAULT FALSE,
                    is_gallery BOOLEAN DEFAULT FALSE,
                    media JSONB,
                    link_flair_text VARCHAR(200),
                    link_flair_css_class VARCHAR(100),
                    is_edited BOOLEAN DEFAULT FALSE,
                    edited_at TIMESTAMP,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    is_removed BOOLEAN DEFAULT FALSE,
                    stickied BOOLEAN DEFAULT FALSE,
                    locked BOOLEAN DEFAULT FALSE,
                    spoiler BOOLEAN DEFAULT FALSE,
                    nsfw BOOLEAN DEFAULT FALSE,
                    created_utc TIMESTAMP,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    content_hash VARCHAR(32),
                    comment_ids TEXT[],
                    comments_fetched_at TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_posts_subreddit ON posts(subreddit);
                CREATE INDEX IF NOT EXISTS idx_posts_created_utc ON posts(created_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_posts_score ON posts(score DESC);
                CREATE INDEX IF NOT EXISTS idx_posts_author ON posts(author);
                CREATE INDEX IF NOT EXISTS idx_posts_flair ON posts(link_flair_text);
                CREATE INDEX IF NOT EXISTS idx_posts_fetched_at ON posts(fetched_at DESC);
                
                CREATE INDEX IF NOT EXISTS idx_posts_fulltext ON posts 
                    USING gin(to_tsvector('english', title || ' ' || COALESCE(selftext, '')));
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    id SERIAL PRIMARY KEY,
                    comment_id VARCHAR(20) UNIQUE NOT NULL,
                    post_id VARCHAR(20) NOT NULL,
                    parent_id VARCHAR(20),
                    body TEXT,
                    body_html TEXT,
                    author VARCHAR(100),
                    author_is_deleted BOOLEAN DEFAULT FALSE,
                    score INTEGER DEFAULT 0,
                    upvote_ratio FLOAT,
                    is_controversial BOOLEAN DEFAULT FALSE,
                    gilded INTEGER DEFAULT 0,
                    is_submitter BOOLEAN DEFAULT FALSE,
                    is_edited BOOLEAN DEFAULT FALSE,
                    edited_at TIMESTAMP,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    is_removed BOOLEAN DEFAULT FALSE,
                    stickied BOOLEAN DEFAULT FALSE,
                    depth INTEGER DEFAULT 0,
                    reply_count INTEGER DEFAULT 0,
                    created_utc TIMESTAMP,
                    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    content_hash VARCHAR(32),
                    subreddit VARCHAR(100)
                );
                
                CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id);
                CREATE INDEX IF NOT EXISTS idx_comments_parent_id ON comments(parent_id);
                CREATE INDEX IF NOT EXISTS idx_comments_author ON comments(author);
                CREATE INDEX IF NOT EXISTS idx_comments_subreddit ON comments(subreddit);
                CREATE INDEX IF NOT EXISTS idx_comments_created_utc ON comments(created_utc DESC);
                CREATE INDEX IF NOT EXISTS idx_comments_score ON comments(score DESC);
                CREATE INDEX IF NOT EXISTS idx_comments_depth ON comments(depth);
                
                CREATE INDEX IF NOT EXISTS idx_comments_fulltext ON comments 
                    USING gin(to_tsvector('english', COALESCE(body, '')));
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS crawl_states (
                    id SERIAL PRIMARY KEY,
                    subreddit VARCHAR(100) UNIQUE NOT NULL,
                    last_post_id VARCHAR(20),
                    after_token VARCHAR(100),
                    posts_crawled INTEGER DEFAULT 0,
                    comments_crawled INTEGER DEFAULT 0,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_activity TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_complete BOOLEAN DEFAULT FALSE
                );
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS change_log (
                    id SERIAL PRIMARY KEY,
                    content_type VARCHAR(20) NOT NULL,
                    content_id VARCHAR(20) NOT NULL,
                    change_type VARCHAR(50) NOT NULL,
                    old_value JSONB,
                    new_value JSONB,
                    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                
                CREATE INDEX IF NOT EXISTS idx_changelog_content_type ON change_log(content_type);
                CREATE INDEX IF NOT EXISTS idx_changelog_content_id ON change_log(content_id);
                CREATE INDEX IF NOT EXISTS idx_changelog_changed_at ON change_log(changed_at DESC);
            """)
            
            self.conn.commit()
            logger.info("Database tables and indexes created")
    
    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def upsert_post(self, post: Post) -> Tuple[bool, str]:
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT content_hash, score, num_comments, selftext, first_seen FROM posts WHERE post_id = %s",
                    (post.post_id,)
                )
                existing = cur.fetchone()
                
                if existing:
                    if existing['content_hash'] != post.content_hash:
                        self._log_change(
                            content_type="post",
                            content_id=post.post_id,
                            change_type="content_updated",
                            old_value={
                                "content_hash": existing['content_hash'],
                                "score": existing['score'],
                                "num_comments": existing['num_comments'],
                                "selftext": (existing['selftext'] or "")[:500]
                            },
                            new_value={
                                "content_hash": post.content_hash,
                                "score": post.score,
                                "num_comments": post.num_comments,
                                "selftext": post.selftext[:500]
                            }
                        )
                        
                        cur.execute("""
                            UPDATE posts SET
                                permalink = %s, url = %s, title = %s, selftext = %s,
                                selftext_html = %s, author = %s, author_is_deleted = %s,
                                subreddit = %s, subreddit_id = %s, score = %s,
                                upvote_ratio = %s, num_comments = %s, gilded = %s,
                                is_self = %s, is_video = %s, is_gallery = %s,
                                media = %s, link_flair_text = %s, link_flair_css_class = %s,
                                is_edited = %s, edited_at = %s, is_deleted = %s,
                                is_removed = %s, stickied = %s, locked = %s,
                                spoiler = %s, nsfw = %s, last_updated = %s,
                                content_hash = %s
                            WHERE post_id = %s
                        """, (
                            post.permalink, post.url, post.title, post.selftext,
                            post.selftext_html, post.author, post.author_is_deleted,
                            post.subreddit, post.subreddit_id, post.score,
                            post.upvote_ratio, post.num_comments, post.gilded,
                            post.is_self, post.is_video, post.is_gallery,
                            json.dumps(post.media.to_dict()) if post.media else None,
                            post.link_flair_text, post.link_flair_css_class,
                            post.is_edited, post.edited_at, post.is_deleted,
                            post.is_removed, post.stickied, post.locked,
                            post.spoiler, post.nsfw, datetime.utcnow(),
                            post.content_hash, post.post_id
                        ))
                        self.conn.commit()
                        self.stats["posts_updated"] += 1
                        return True, "updated"
                    else:
                        cur.execute(
                            "UPDATE posts SET last_updated = %s WHERE post_id = %s",
                            (datetime.utcnow(), post.post_id)
                        )
                        self.conn.commit()
                        return False, "unchanged"
                else:
                    cur.execute("""
                        INSERT INTO posts (
                            post_id, permalink, url, title, selftext, selftext_html,
                            author, author_is_deleted, subreddit, subreddit_id,
                            score, upvote_ratio, num_comments, gilded,
                            is_self, is_video, is_gallery, media,
                            link_flair_text, link_flair_css_class,
                            is_edited, edited_at, is_deleted, is_removed,
                            stickied, locked, spoiler, nsfw,
                            created_utc, fetched_at, first_seen, last_updated, content_hash
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s
                        )
                    """, (
                        post.post_id, post.permalink, post.url, post.title,
                        post.selftext, post.selftext_html, post.author,
                        post.author_is_deleted, post.subreddit, post.subreddit_id,
                        post.score, post.upvote_ratio, post.num_comments, post.gilded,
                        post.is_self, post.is_video, post.is_gallery,
                        json.dumps(post.media.to_dict()) if post.media else None,
                        post.link_flair_text, post.link_flair_css_class,
                        post.is_edited, post.edited_at, post.is_deleted, post.is_removed,
                        post.stickied, post.locked, post.spoiler, post.nsfw,
                        post.created_utc, datetime.utcnow(), datetime.utcnow(),
                        datetime.utcnow(), post.content_hash
                    ))
                    self.conn.commit()
                    self.stats["posts_inserted"] += 1
                    return True, "inserted"
                    
        except psycopg2.IntegrityError:
            self.conn.rollback()
            logger.warning(f"Duplicate post ID: {post.post_id}")
            return False, "duplicate"
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error upserting post: {e}")
            return False, "error"
    
    def get_post(self, post_id: str) -> Optional[Dict[str, Any]]:
        """Get a post by ID."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM posts WHERE post_id = %s", (post_id,))
            return cur.fetchone()
    
    def get_posts(
        self,
        subreddit: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
        sort_by: str = "created_utc",
        sort_order: str = "DESC",
        min_score: Optional[int] = None,
        flair: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Get posts with filtering (ML-friendly query interface).
        """
        conditions = []
        params = []
        
        if subreddit:
            conditions.append("subreddit = %s")
            params.append(subreddit)
        if min_score is not None:
            conditions.append("score >= %s")
            params.append(min_score)
        if flair:
            conditions.append("link_flair_text = %s")
            params.append(flair)
        if since:
            conditions.append("created_utc >= %s")
            params.append(since)
        if until:
            conditions.append("created_utc <= %s")
            params.append(until)
        
        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        
        valid_sort_columns = ['created_utc', 'score', 'num_comments', 'fetched_at']
        if sort_by not in valid_sort_columns:
            sort_by = 'created_utc'
        
        sort_order = "DESC" if sort_order.upper() == "DESC" else "ASC"
        
        query = f"""
            SELECT * FROM posts 
            WHERE {where_clause}
            ORDER BY {sort_by} {sort_order}
            LIMIT %s OFFSET %s
        """
        params.extend([limit, skip])
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    
    def search_posts(self, text_query: str, limit: int = 100) -> List[Dict[str, Any]]:  
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *, ts_rank(
                    to_tsvector('english', title || ' ' || COALESCE(selftext, '')),
                    plainto_tsquery('english', %s)
                ) as rank
                FROM posts
                WHERE to_tsvector('english', title || ' ' || COALESCE(selftext, '')) 
                    @@ plainto_tsquery('english', %s)
                ORDER BY rank DESC
                LIMIT %s
            """, (text_query, text_query, limit))
            return cur.fetchall()
    
    def upsert_comment(self, comment: Comment) -> Tuple[bool, str]:
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT content_hash, score, body, first_seen FROM comments WHERE comment_id = %s",
                    (comment.comment_id,)
                )
                existing = cur.fetchone()
                
                if existing:
                    if existing['content_hash'] != comment.content_hash:
                        self._log_change(
                            content_type="comment",
                            content_id=comment.comment_id,
                            change_type="content_updated",
                            old_value={
                                "content_hash": existing['content_hash'],
                                "score": existing['score'],
                                "body": (existing['body'] or "")[:500]
                            },
                            new_value={
                                "content_hash": comment.content_hash,
                                "score": comment.score,
                                "body": comment.body[:500]
                            }
                        )
                        
                        cur.execute("""
                            UPDATE comments SET
                                post_id = %s, parent_id = %s, body = %s, body_html = %s,
                                author = %s, author_is_deleted = %s, score = %s,
                                is_controversial = %s, gilded = %s, is_submitter = %s,
                                is_edited = %s, edited_at = %s, is_deleted = %s,
                                is_removed = %s, stickied = %s, depth = %s,
                                reply_count = %s, last_updated = %s, content_hash = %s,
                                subreddit = %s
                            WHERE comment_id = %s
                        """, (
                            comment.post_id, comment.parent_id, comment.body,
                            comment.body_html, comment.author, comment.author_is_deleted,
                            comment.score, comment.is_controversial, comment.gilded,
                            comment.is_submitter, comment.is_edited, comment.edited_at,
                            comment.is_deleted, comment.is_removed, comment.stickied,
                            comment.depth, comment.reply_count, datetime.utcnow(),
                            comment.content_hash, comment.subreddit, comment.comment_id
                        ))
                        self.conn.commit()
                        self.stats["comments_updated"] += 1
                        return True, "updated"
                    else:
                        cur.execute(
                            "UPDATE comments SET last_updated = %s WHERE comment_id = %s",
                            (datetime.utcnow(), comment.comment_id)
                        )
                        self.conn.commit()
                        return False, "unchanged"
                else:
                    cur.execute("""
                        INSERT INTO comments (
                            comment_id, post_id, parent_id, body, body_html,
                            author, author_is_deleted, score, is_controversial,
                            gilded, is_submitter, is_edited, edited_at,
                            is_deleted, is_removed, stickied, depth,
                            reply_count, created_utc, fetched_at, first_seen,
                            last_updated, content_hash, subreddit
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s
                        )
                    """, (
                        comment.comment_id, comment.post_id, comment.parent_id,
                        comment.body, comment.body_html, comment.author,
                        comment.author_is_deleted, comment.score, comment.is_controversial,
                        comment.gilded, comment.is_submitter, comment.is_edited,
                        comment.edited_at, comment.is_deleted, comment.is_removed,
                        comment.stickied, comment.depth, comment.reply_count,
                        comment.created_utc, datetime.utcnow(), datetime.utcnow(),
                        datetime.utcnow(), comment.content_hash, comment.subreddit
                    ))
                    
                    cur.execute("""
                        UPDATE posts 
                        SET comment_ids = array_append(
                            COALESCE(comment_ids, ARRAY[]::TEXT[]), %s
                        ),
                        comments_fetched_at = %s
                        WHERE post_id = %s AND NOT (%s = ANY(COALESCE(comment_ids, ARRAY[]::TEXT[])))
                    """, (comment.comment_id, datetime.utcnow(), comment.post_id, comment.comment_id))
                    
                    self.conn.commit()
                    self.stats["comments_inserted"] += 1
                    return True, "inserted"
                    
        except psycopg2.IntegrityError:
            self.conn.rollback()
            logger.warning(f"Duplicate comment ID: {comment.comment_id}")
            return False, "duplicate"
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Error upserting comment: {e}")
            return False, "error"
    
    def get_comment(self, comment_id: str) -> Optional[Dict[str, Any]]:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM comments WHERE comment_id = %s", (comment_id,))
            return cur.fetchone()
    
    def get_comments_for_post(
        self,
        post_id: str,
        limit: int = 1000,
        sort_by: str = "created_utc",
        sort_order: str = "ASC"
    ) -> List[Dict[str, Any]]:
        """Get all comments for a specific post."""
        valid_sort_columns = ['created_utc', 'score', 'depth']
        if sort_by not in valid_sort_columns:
            sort_by = 'created_utc'
        sort_order = "DESC" if sort_order.upper() == "DESC" else "ASC"
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT * FROM comments 
                WHERE post_id = %s
                ORDER BY {sort_by} {sort_order}
                LIMIT %s
            """, (post_id, limit))
            return cur.fetchall()
    
    def get_comment_thread(self, parent_id: str) -> List[Dict[str, Any]]:
        """Get all replies to a specific comment."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM comments WHERE parent_id = %s", (parent_id,))
            return cur.fetchall()
    
    def get_comments(
        self,
        subreddit: Optional[str] = None,
        limit: int = 100,
        skip: int = 0,
        min_score: Optional[int] = None,
        max_depth: Optional[int] = None,
        since: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        conditions = []
        params = []
        
        if subreddit:
            conditions.append("subreddit = %s")
            params.append(subreddit)
        if min_score is not None:
            conditions.append("score >= %s")
            params.append(min_score)
        if max_depth is not None:
            conditions.append("depth <= %s")
            params.append(max_depth)
        if since:
            conditions.append("created_utc >= %s")
            params.append(since)
        
        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        
        query = f"""
            SELECT * FROM comments 
            WHERE {where_clause}
            ORDER BY created_utc DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, skip])
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
    
    def search_comments(self, text_query: str, limit: int = 100) -> List[Dict[str, Any]]:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *, ts_rank(
                    to_tsvector('english', COALESCE(body, '')),
                    plainto_tsquery('english', %s)
                ) as rank
                FROM comments
                WHERE to_tsvector('english', COALESCE(body, '')) 
                    @@ plainto_tsquery('english', %s)
                ORDER BY rank DESC
                LIMIT %s
            """, (text_query, text_query, limit))
            return cur.fetchall()
    
    def _log_change(
        self,
        content_type: str,
        content_id: str,
        change_type: str,
        old_value: Dict[str, Any],
        new_value: Dict[str, Any]
    ):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO change_log (content_type, content_id, change_type, old_value, new_value, changed_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    content_type, content_id, change_type,
                    json.dumps(old_value), json.dumps(new_value),
                    datetime.utcnow()
                ))
            self.stats["changes_logged"] += 1
        except Exception as e:
            logger.error(f"Error logging change: {e}")
    
    def get_changes(
        self,
        content_type: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        conditions = []
        params = []
        
        if content_type:
            conditions.append("content_type = %s")
            params.append(content_type)
        if since:
            conditions.append("changed_at >= %s")
            params.append(since)
        
        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT * FROM change_log 
                WHERE {where_clause}
                ORDER BY changed_at DESC
                LIMIT %s
            """, params + [limit])
            return cur.fetchall()

    def get_crawl_state(self, subreddit: str) -> Optional[CrawlState]:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM crawl_states WHERE subreddit = %s", (subreddit,))
            doc = cur.fetchone()
            
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
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO crawl_states (
                    subreddit, last_post_id, after_token, posts_crawled,
                    comments_crawled, started_at, last_activity, is_complete
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (subreddit) DO UPDATE SET
                    last_post_id = EXCLUDED.last_post_id,
                    after_token = EXCLUDED.after_token,
                    posts_crawled = EXCLUDED.posts_crawled,
                    comments_crawled = EXCLUDED.comments_crawled,
                    last_activity = EXCLUDED.last_activity,
                    is_complete = EXCLUDED.is_complete
            """, (
                state.subreddit, state.last_post_id, state.after_token,
                state.posts_crawled, state.comments_crawled,
                state.started_at, state.last_activity, state.is_complete
            ))
            self.conn.commit()
    
    def reset_crawl_state(self, subreddit: str):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM crawl_states WHERE subreddit = %s", (subreddit,))
            self.conn.commit()
    
    def export_for_ml(
        self,
        subreddit: Optional[str] = None,
        include_comments: bool = True,
        flatten: bool = False
    ) -> List[Dict[str, Any]]:
        conditions = []
        params = []
        
        if subreddit:
            conditions.append("subreddit = %s")
            params.append(subreddit)
        
        where_clause = " AND ".join(conditions) if conditions else "TRUE"
        
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM posts WHERE {where_clause}", params)
            posts = [dict(row) for row in cur.fetchall()]
            
            if not include_comments:
                return posts
            
            if flatten:
                cur.execute(f"SELECT * FROM comments WHERE {where_clause}", params)
                comments = [dict(row) for row in cur.fetchall()]
                return posts + comments
            
            for post in posts:
                cur.execute(
                    "SELECT * FROM comments WHERE post_id = %s ORDER BY created_utc",
                    (post['post_id'],)
                )
                post['comments'] = [dict(row) for row in cur.fetchall()]
            
            return posts
    
    def get_stats(self) -> Dict[str, Any]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM posts")
            total_posts = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM comments")
            total_comments = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM change_log")
            total_changes = cur.fetchone()[0]
        
        return {
            **self.stats,
            "total_posts": total_posts,
            "total_comments": total_comments,
            "total_changes": total_changes
        }
    
    def reset_stats(self):
        self.stats = {
            "posts_inserted": 0,
            "posts_updated": 0,
            "comments_inserted": 0,
            "comments_updated": 0,
            "changes_logged": 0
        }
