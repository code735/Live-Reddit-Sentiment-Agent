import pathway as pw

# Define schema matching your JSON
class RedditSentimentSchema(pw.Schema):
    event_id: str
    post_id: str
    event_type: str
    ticker: str
    text: str
    sentiment: str
    confidence: float
    observed_at: str

# Fake test data (simulates JSONL rows)
data = [
    ("1", "p1", "upsert", "TSLA", "Good earnings", "positive", 0.9, "2026-01-01T10:00:00Z"),
    ("2", "p2", "upsert", "TSLA", "Too expensive", "negative", 0.8, "2026-01-01T10:01:00Z"),
    ("3", "p3", "upsert", "TSLA", "Holding long term", "neutral", 0.55, "2026-01-01T10:02:00Z"),
]

# Create a Pathway table from rows
table = pw.debug.table_from_rows(schema=RedditSentimentSchema, rows=data, mode="streaming")

# Deduplicate by event_id so Pathway can track new rows
table = table.with_id_from(table.event_id)

# Print all rows (simulate streaming / new events)
pw.debug.compute_and_print(table)

# Start Pathway engine (required)
pw.run()
