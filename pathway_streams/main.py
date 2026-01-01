import pathway as pw
import os
from schema import RedditSentimentSchema

print("Pathway version:", pw.__version__)
print(os.getcwd())

table = pw.io.jsonlines.read("./pathway_streams/data_stream/", schema=RedditSentimentSchema)

latest_table = table.groupby(table.post_id).reduce(
    post_id=table.post_id,
    sentiment=pw.reducers.latest(table.sentiment),
    text=pw.reducers.latest(table.text),
)

pw.io.csv.write(table, "./output.csv")
# pw.debug.compute_and_print(table)
pw.run()