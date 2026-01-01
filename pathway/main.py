import pathway as pw
import os
from schema import RedditSentimentSchema

print("Pathway version:", pw.__version__)
print(os.getcwd())

table = pw.io.jsonlines.read("./pathway/data_stream/", schema=RedditSentimentSchema)
pw.io.csv.write(table, "./output.csv")
pw.debug.compute_and_print(table)
pw.run()