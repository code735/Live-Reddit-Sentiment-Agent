import os
from dotenv import load_dotenv
from google import genai
from google.genai import types
import argparse
from prompts import system_prompt
from agent.functions.fetch_recent_news import (
    fetch_recent_news,
    schema_fetch_recent_news,
)
from agent.functions.fetch_price_context import (
    fetch_price_context_indian,
    schema_fetch_price_context_indian,
)
from agent.functions.re_evaluate_sentiment_confidence import (
    re_evaluate_sentiment_confidence,
    schema_re_evaluate_sentiment_confidence,
)
from agent.functions.alert_dashboard import alert_dashboard, schema_alert_dashboard
from agent.functions.no_alert import no_alert, schema_no_alert

load_dotenv()
api_key = os.environ.get("GEMINI_API_KEY")

client = genai.Client(api_key=api_key)

parser = argparse.ArgumentParser(
    description="Takes a question or a prompt from user as a command line argument"
)
parser.add_argument("--verbose", action="store_true")
parser.add_argument("user_prompt", help="user prompt")

args = parser.parse_args()

user_prompt = args.user_prompt

messages = [
    types.Content(role="user", parts=[types.Part(text=user_prompt)]),
]

available_functions = types.Tool(
    function_declarations=[
        schema_fetch_recent_news,
        schema_fetch_price_context_indian,
        schema_re_evaluate_sentiment_confidence,
        schema_alert_dashboard,
    ]
)

FUNCTION_REGISTRY = {
    "fetch_price_context_indian": fetch_price_context_indian,
    "fetch_recent_news": fetch_recent_news,
    "re_evaluate_sentiment_confidence": re_evaluate_sentiment_confidence,
    "alert_dashboard": alert_dashboard,
    "no_alert": no_alert,
}


def extract_function_calls(response):
    calls = []

    for candidate in response.candidates:
        content = candidate.content
        if not content or not content.parts:
            continue

        for part in content.parts:
            if part.function_call is not None:
                calls.append(part.function_call)

    return calls


def execute_function_call(fc):
    fn_name = fc.name
    fn_args = fc.args or {}

    if fn_name not in FUNCTION_REGISTRY:
        raise ValueError(f"Unknown function: {fn_name}")

    fn = FUNCTION_REGISTRY[fn_name]
    result = fn(**fn_args)

    # ✅ ensure result is never None
    if result is None:
        result = {"reason": "default fallback"}

    return fn_name, result


def agent_tool_call_loop(client, messages, max_iterations=20, verbose=False):
    for i in range(max_iterations):
        try:
            response = client.models.generate_content(
                model="gemini-2.0-flash-001",
                contents=messages,
                config=types.GenerateContentConfig(
                    tools=[available_functions],
                    system_instruction=system_prompt,
                ),
            )

            if verbose and response.usage_metadata:
                print("Prompt tokens:", response.usage_metadata.prompt_token_count)
                print(
                    "Response tokens:", response.usage_metadata.candidates_token_count
                )

            # Add model responses to conversation
            for candidate in response.candidates:
                messages.append(candidate.content)

            function_calls = extract_function_calls(response)

            # 🔴 CASE 1: model wants tools
            if function_calls:
                for fc in function_calls:
                    print("Calling function:", fc.name)
                    # print("response:", response)
                    print(response.text)
                    print("prompt tokens: ", response.usage_metadata.prompt_token_count)
                    print("response tokes: ", response.usage_metadata.candidates_token_count)
                    # ← prints the function being called
                    name, result = execute_function_call(fc)

                    tool_part = types.Part(
                        function_response=types.FunctionResponse(
                            name=name,
                            response=result,
                        )
                    )

                    messages.append(
                        types.Content(
                            role="tool",
                            parts=[tool_part],
                        )
                    )

                continue  # ← let the model think again

            # if not any(
            #     fc.name in ["alert_dashboard", "no_alert"] for fc in function_calls
            # ):
            #     name = "no_alert"
            #     result = "reason"
            #     # Fallback if model didn’t choose either
            #     messages.append(
            #         types.Content(
            #             role="tool",
            #             parts=[
            #                 types.Part(
            #                     function_response=types.FunctionResponse(
            #                         name=name,
            #                         response=result
            #                     )
            #                 )
            #             ],
            #         )
            #     )

            # 🟢 CASE 2: no function calls → text is final
            if response.text:
                if verbose:
                    print("Final response:")
                return response.text

            # ⚠️ CASE 3: nothing usable
            if verbose:
                print("No text or function calls returned; stopping.")
            return None

        except Exception as e:
            print(f"Error: {e}")
            return None

    print(f"Reached maximum iterations ({max_iterations}).")
    return None


agent_tool_call_loop(client, messages)


# print("model response: ", response.text)
# print("prompt tokens: ", response.usage_metadata.prompt_token_count)
# print("response tokes: ", response.usage_metadata.candidates_token_count)
