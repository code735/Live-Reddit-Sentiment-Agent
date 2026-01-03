from google.genai import types

def alert_dashboard(alert_type, confidence, rationale, ticker_obj):
  # print("ticker_obj from alert",ticker_obj)
  return {alert_type, confidence, rationale, ticker_obj}

schema_alert_dashboard = types.FunctionDeclaration(
    name="alert_dashboard",
    description="Alerts the dashboard with alert details",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "alert_type": types.Schema(
                type=types.Type.STRING,
                description="Type of alert",
            ),
            "confidence": types.Schema(
                type=types.Type.NUMBER,
                description="Confidence level of the alert",
            ),
            "rationale": types.Schema(
                type=types.Type.STRING,
                description="Reason for the alert",
            ),
            "ticker": types.Schema(
                type=types.Type.STRING,
                description="Stock ticker symbol",
            ),
        },
        required=["alert_type", "confidence", "rationale", "ticker"]
    )
)
