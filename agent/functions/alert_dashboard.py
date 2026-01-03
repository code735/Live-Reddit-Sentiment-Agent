def alert_dashboard(alert_type, confidence, rationale, ticker_obj):
  # print("ticker_obj from alert",ticker_obj)
  return {alert_type, confidence, rationale, ticker_obj}

schema_alert_dashboard = types.FunctionDeclaration(
    name="alert_dashboard",
    description="alerts the dashboard and returns the alert_type, confidence, rationale and ticker_obj",
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