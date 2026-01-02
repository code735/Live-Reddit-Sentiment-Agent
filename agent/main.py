import google.generativeai as genai
import os

genai.configure(
    api_key=os.environ["GEMINI_API_KEY"]
)

model = genai.GenerativeModel(
    model_name="gemini-2.5-flash"
)


tools = [
    {
        "function_declarations": [
            {
                "name": "fetch_news",
                "description": "Fetch latest news about a stock ticker",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string"}
                    },
                    "required": ["ticker"]
                }
            },
            {
                "name": "fetch_fundamentals",
                "description": "Fetch fundamentals for a stock ticker",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ticker": {"type": "string"}
                    },
                    "required": ["ticker"]
                }
            }
        ]
    }
]
