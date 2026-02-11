# Live-Reddit-Sentiment-Agent
We propose a live agentic system that continuously ingests Reddit discussions, updates sentiment signals in real time, and reacts autonomously to meaningful changes in market narratives.


<img width="3803" height="3715" alt="image (4)" src="https://github.com/user-attachments/assets/702e1ba5-efba-401d-9cce-016381aeca0b" />

---

## Setup
1. Install Python 3.12 (required by `pyproject.toml`).
2. Create and activate a virtual environment:
```bash
python3.12 -m venv .venv
source .venv/bin/activate
```
3. Install dependencies:
```bash
pip install -e .
```
4. Optional (Gemini-backed sentiment):
```bash
cp sentiment/.env.example sentiment/.env
```
Set `GEMINI_API_KEY` in `sentiment/.env`.

## Run
1. Start the API server:
```bash
uvicorn server.main:app
```

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


future improvements -
1. RAG for finding information on dedicated tickers throughout reddit.
2. market prediction history
3. accept gemini_api_key from user.'
