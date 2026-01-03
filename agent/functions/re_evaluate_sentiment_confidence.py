from fetch_recent_news import fetch_recent_news
from sentiment.app.services.sentiment import SentimentService

service = SentimentService()


def re_evaluate_sentiment_confidence(text):
    return service.analyze(str(text))


# converted_string = fetch_recent_news("RADHIKAJWE")
# reevaluation = re_evaluate_sentiment_confidence(converted_string)

# print(reevaluation)


schema_re_evaluate_sentiment_confidence = types.FunctionDeclaration(
    name="re_evaluate_sentiment_confidence",
    description="fetches re-evaluated confidence score and sentiment label",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "ticker": types.Schema(
                type=types.Type.STRING,
                description="stock ticker",
            )
        },
        required=["ticker"]
    ),
)