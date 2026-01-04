import pathway as pw
import os
from .schema import RedditSentimentSchema
from agent.main import on_new_rows

print("Pathway version:", pw.__version__)
print(os.getcwd())

table = pw.io.jsonlines.read(
    "./pathway_streams/data_stream/",
    schema=RedditSentimentSchema
)

latest_table = table.groupby(table.post_id).reduce(
    post_id=table.post_id,
    sentiment=pw.reducers.latest(table.sentiment),
    text=pw.reducers.latest(table.text),
)

filtered_table = table.filter(table.ticker != "UNKNOWN")

pw.io.csv.write(filtered_table, "./output.csv")
pw.io.subscribe(filtered_table, on_new_rows)
pw.run()
