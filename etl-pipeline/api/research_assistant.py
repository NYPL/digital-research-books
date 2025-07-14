from typing import Optional
from .elastic import ElasticClient, SearchParams
from langgraph.prebuilt import create_react_agent
from langchain.chat_models import init_chat_model
from langchain.schema.messages import SystemMessage, ToolMessage, HumanMessage
from langchain_core.messages import messages_to_dict, messages_from_dict
from langchain.agents import tool
from logger import create_log

VRA_SYSTEM_PROMPT_V0 = SystemMessage(
    content=(
        "You are a Virtual Research Assistant for the New York Public Library. "
        "Find relevant digitized literature using the search-tool based on the patron's inquiry. "
        "Respond politely with a brief description of how you searched. "
        "If the inquiry is not research related, politely decline to answer."
    )
)

logger = create_log(__name__)


class ResearchAssistant:
    def __init__(self, es_client: ElasticClient):
        @tool(
            "search-tool",
            description="Search the Digital Research Books catalog.",
            args_schema=SearchParams,
        )
        def search_catalog(
            title: Optional[str] = None,
            keyword: Optional[str] = None,
            subject: Optional[str] = None,
            author: Optional[str] = None,
            publication_year_start: Optional[str] = None,
            publication_year_end: Optional[str] = None,
            languages: Optional[list[str]] = None,
        ):
            params = SearchParams(
                title=title,
                keyword=keyword,
                subject=subject,
                author=author,
                publication_year_start=publication_year_start,
                publication_year_end=publication_year_end,
                languages=languages,
            )

            logger.info(f"Calling search-tool with params: {params}")

            return es_client.search_catalog(params)

        self.model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")
        self.agent = create_react_agent(
            model=self.model,
            tools=[search_catalog],
            prompt=self.system_prompt,
        )

    @property
    def system_prompt(self):
        return VRA_SYSTEM_PROMPT_V0

    def get_chat_completion(self, messages):
        parsed_messages = self._parse_messages(messages)
        response = self.agent.invoke({"messages": parsed_messages})
        results = None

        for message in response["messages"]:
            message.pretty_print()

            if isinstance(message, ToolMessage):
                results = message.content

        return {
            "answer": response["messages"][-1].content,
            "results": results,
            "messages": messages_to_dict(parsed_messages),
        }

    def _parse_messages(self, messages):
        parsed_messages = messages_from_dict(messages)

        if not isinstance(parsed_messages[0], SystemMessage):
            parsed_messages.insert(0, self.system_prompt)

        return parsed_messages
