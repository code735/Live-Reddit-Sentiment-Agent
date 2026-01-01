import pathway as pw
from schema import RedditSentimentSchema

print("pathway version", pw.__version__)


events = pw.io.jsonlines.read(
    path="../data_stream/",
    schema=RedditSentimentSchema,
    mode="streaming",
    with_metadata=True,
)

# IMPORTANT: stable IDs → only new rows are printed
events = events.with_id_from(events.event_id)

# Print ONLY incremental updates
pw.debug.compute_and_print(events)

pw.run()