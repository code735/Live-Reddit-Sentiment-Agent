import pathway as pw
import os
from .schema import RedditSentimentSchema


def start_pawthway():
    print("Pathway version:", pw.__version__)
    print(os.getcwd())

    table = pw.io.jsonlines.read(
        "./pathway_streams/data_stream/",
        schema=RedditSentimentSchema,
    )

    # Keep only the first time each post_id appears
    deduplicated_table = table.groupby(table.post_id).reduce(
        post_id=table.post_id,
        sentiment=pw.reducers.earliest(table.sentiment),
        text=pw.reducers.earliest(table.text),
        ticker=pw.reducers.earliest(table.ticker),
    )

    # Apply your filter AFTER deduplication
    filtered_table = deduplicated_table.filter(
        deduplicated_table.ticker != "UNKNOWN"
    )

    pw.io.csv.write(filtered_table, "./output.csv")

    pw.run(with_dashboard=False)
