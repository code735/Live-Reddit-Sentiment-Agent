from google.genai import types


def no_alert(reason="default reason"):
  return reason

schema_no_alert = types.FunctionDeclaration(
    name="no_alert",
    description="Call when no alert is required",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "reason": types.Schema(type=types.Type.STRING, description="Reason for no alert")
        },
        required=["reason"]
    )
)