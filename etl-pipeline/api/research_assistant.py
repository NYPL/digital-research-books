from .elastic import ElasticClient
from langgraph.prebuilt import create_react_agent
from langchain.chat_models import init_chat_model
from langchain.schema.messages import SystemMessage
from langchain_core.messages import convert_to_messages
from langchain.agents import Tool

VRA_SYSTEM_PROMPT_V0 = SystemMessage(
    content=(
        "You are a Virtual Research Assistant. "
        "Find relevant digitized books using the search tool based on the patron's inquiry. "
        "Respond with a brief description of what you did. "
        "If the inquiry is not related to the library catalog, ask for further details or politely decline to fulfill their request."
    )
)


class ResearchAssistant:
    def __init__(self, es_client: ElasticClient):
        self.tools = [
            Tool.from_function(
                func=es_client.searchQuery,
                name="search",
                description=(
                    "Use a JSON object with a query key, whose value is a list of [field, value] to search. "
                    "Valid fields are title, author, and subject. "
                    "Example inputs: "
                    '{"query": [["title", "Giovanni\'s Room"], ["author", "James Baldwin"]] }'
                )
            )
        ]
        self.model = init_chat_model("gemini-1.5-flash", model_provider="google_genai")
        self.agent = create_react_agent(
            model=self.model,
            tools=self.tools,
            prompt=self.system_prompt,
        )
    
    @property
    def system_prompt(self):
        return VRA_SYSTEM_PROMPT_V0

    def get_chat_completion(self, messages):
        parsed_messages = self._parse_messages(messages)
        response = self.agent.invoke({ "messages": parsed_messages })

        for message in response["messages"]:
            message.pretty_print()

            if message.type == "tool_result":
                results = message.content

        return {
            "content": response["messages"][-1].content,
            "results": results
        }
    
    def _parse_messages(self, messages):
        parsed_messages = convert_to_messages(messages)

        if not isinstance(parsed_messages[0], SystemMessage):
            parsed_messages.insert(0, self.system_prompt)

        return parsed_messages
