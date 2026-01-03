def no_alert(reason, ticker_obj):
  return { message: "no alert sorry" }

schema_no_alert = types.FunctionDeclaration(
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