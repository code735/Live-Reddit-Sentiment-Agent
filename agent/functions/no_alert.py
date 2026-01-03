from google.genai import types


def no_alert(reason):
    return reason

# print(no_alert("reason"))

schema_no_alert = types.FunctionDeclaration(
    name="no_alert",
    description="no alert to dashboard just state a reason and it'll print the reason",
    parameters=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "reason": types.Schema(
                type=types.Type.STRING,
                description="LLM will return a reason for why no alert is required here in string datatype.",
            )
        },
        required=["reason"]
    ),
)