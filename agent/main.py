import os
from dotenv import load_dotenv
from google import genai
from google.genai import types
import argparse
from prompts import system_prompt

load_dotenv()
api_key = os.environ.get("GEMINI_API_KEY")

client = genai.Client(api_key=api_key)

parser = argparse.ArgumentParser(description="Takes a question or a prompt from user as a command line argument")
parser.add_argument("--verbose", action="store_true")
parser.add_argument("user_prompt", help="user prompt")

args = parser.parse_args()

if args.verbose:
  user_prompt = args.user_prompt

messages = [
  types.Content(role="user", parts=[types.Part(text=user_prompt)]),
]

response = client.models.generate_content(
    model="gemini-2.0-flash-001",
    contents=messages,
    config=types.GenerateContentConfig(system_instruction=system_prompt)
)


print("model response: ",response.text)
print("model response prompt_token_count: ",response.usage_metadata.prompt_token_count)
print("model response candidates_token_count: ",response.usage_metadata.candidates_token_count)
