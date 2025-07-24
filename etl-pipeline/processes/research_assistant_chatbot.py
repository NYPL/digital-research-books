import requests
import os
from langchain_core.messages import messages_to_dict, HumanMessage


class ResearchAssistantChatBot:
    def __init__(self, *args):
        pass

    def run(self):
        print("Chat Interface")
        print("Type a question and hit Enter. Ctrl+C to exit.\n")

        messages = []

        while True:
            try:
                query = input("Patron: ").strip()

                if not query:
                    continue

                messages.append(messages_to_dict([HumanMessage(content=query)])[0])

                response = requests.put(
                    "http://localhost:5050/chats",
                    json={"messages": messages},
                    headers={"X-API-KEY": os.environ["API_KEY"]},
                )

                if response.status_code != 201:
                    print(f"Error: {response.status_code} - {response.text}")
                    continue

                response_json = response.json()
                messages = response_json["data"]["messages"]
                answer = response_json["data"]["answer"]
                print(f"Research Assistant: {answer}\n")
            except KeyboardInterrupt:
                print("\nExiting.")
                break
