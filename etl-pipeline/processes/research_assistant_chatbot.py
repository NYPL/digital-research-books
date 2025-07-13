import requests
import os


class ResearchAssistantChatBot():
    
    def __init__(self, *args):
        pass

    def runProcess(self):
        print("Chat Interface")
        print("Type a question and hit Enter. Ctrl+C to exit.\n")

        history = []

        while True:
            try:
                query = input("Patron: ").strip()
                
                if not query:
                    continue

                history.append({"role": "user", "content": query})

                response = requests.put("http://localhost:5050/chats", json={"messages": history}, headers={
                    "X-API-KEY": os.environ["API_KEY"]
                })

                if response.status_code != 201:
                    print(f"Error: {response.status_code} - {response.text}")
                    continue

                response_json = response.json()
                answer = response_json["data"]["content"]
                history.append(answer)
                print(f"Research Assistant: {answer}\n")
            except KeyboardInterrupt:
                print("\nExiting.")
                break
