# Live-Reddit-Sentiment-Agent
We propose a live agentic system that continuously ingests Reddit discussions, updates sentiment signals in real time, and reacts autonomously to meaningful changes in market narratives.


<img width="3803" height="3715" alt="image (4)" src="https://github.com/user-attachments/assets/702e1ba5-efba-401d-9cce-016381aeca0b" />

---

## Reddit Crawler

plz see testrun.ipynb to see how to run
### Data Models

**Post Fields:**
- `post_id`, `title`, `selftext`, `author`
- `score`, `upvote_ratio`, `num_comments`
- `link_flair_text`, `created_utc`
- `is_edited`, `is_deleted`, `is_removed`
- `content_hash` (for change detection)

**Comment Fields:**
- `comment_id`, `post_id`, `parent_id`
- `body`, `author`, `score`, `depth`
- `is_submitter`, `is_edited`, `created_utc`
- `content_hash` (for change detection)

