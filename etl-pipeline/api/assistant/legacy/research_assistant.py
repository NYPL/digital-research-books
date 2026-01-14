from typing import Optional
from flask import current_app
from .elastic import ElasticClient, SearchParams
from langgraph.prebuilt import create_react_agent
from langchain.chat_models import init_chat_model
from langchain_core.messages import (
    SystemMessage,
    ToolMessage,
    messages_to_dict,
    messages_from_dict,
)
from langchain.tools import tool
from logger import create_log
from ...utils import APIUtils, json_dump_uuid
from ...db import DBClient
from .item_search import item_full_text_search, QueryMode

import json

VRA_SYSTEM_PROMPT_V0 = SystemMessage(
    content=(
        "You are a Virtual Research Assistant.\n\n"
        "Your tasks are:\n"
        "1. Use the `catalog-search-tool` to find relevant digitized literature (items) based on the patron's inquiry.\n"
        "2. Use the `item-search-tool` to find relevant text within an item that may answer the inquiry. "
        "If an <itemId>item_id</itemId> is provided, use that value as the item_id.\n\n"
        "Guidelines:\n"
        "- Respond politely with a brief description of what you did.\n"
        "- Do not summarize the search results.\n"
        "- If the inquiry is not research-related or not related to a particular item, politely decline to answer."
        "- If a catalog search returns no results, respond in a friendly and apologetic tone."
        "Refer to the user's search term in your response. For example: "
        "\"Sorry, we don't currently have any items on the topic '{search term}'. Ask me about another topic, or try the suggestions below.\""
    )
)

logger = create_log(__name__)


class ResearchAssistant:
    def __init__(self, es_client: ElasticClient, db_client: DBClient):
        @tool(
            "item-search-tool",
            description="Search within an item given a keyword, semantic or hybrid query.",
        )
        def search_item(
            item_id: str,
            query_mode: QueryMode,
            keyword: Optional[str] = None,
            semantic_query: Optional[str] = None,
            size: int = 10,
        ):
            item_results = item_full_text_search(
                item_id,
                query_mode,
                keyword,
                semantic_query,
                size,
            )

            data_block = {"data": item_results, "type": "item_search"}

            return json.dumps(data_block)

        @tool(
            "catalog-search-tool",
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
            page: int = 0,
            size: int = 10,
        ):
            """
            Perform a keyword search on DRB catalog. This is the same search backend
            as used in the DRB site.
            """
            params = SearchParams(
                title=title,
                keyword=keyword,
                subject=subject,
                author=author,
                publication_year_start=publication_year_start,
                publication_year_end=publication_year_end,
                languages=languages,
                page=page,
                size=size,
            )

            logger.info(f"Calling search-tool with params: {params}")

            search_result = es_client.search_catalog(params)
            reader_version = current_app.config["READER_VERSION"]
            db_client.createSession()
            results = []
            for res in search_result.hits:
                edition_ids = [e.edition_id for e in res.meta.inner_hits.editions.hits]

                try:
                    highlights = {
                        key: list(set(res.meta.highlight[key]))
                        for key in res.meta.highlight
                    }
                except AttributeError:
                    highlights = {}

                results.append((res.uuid, edition_ids, highlights))

            if es_client.sortReversed is True:
                results = [r for r in reversed(results)]

            works = db_client.fetchSearchedWorks(results)

            # Depending on the version of elastic search, hits will either be an integer or a dictionary
            total_hits = (
                search_result.hits.total
                if isinstance(search_result.hits.total, int)
                else search_result.hits.total.value
            )

            facets = APIUtils.formatAggregationResult(
                search_result.aggregations.to_dict()
            )
            paging = APIUtils.formatPagingOptions(
                params.page + 1, params.size, total_hits
            )

            search_results = {
                "totalWorks": total_hits,
                "works": APIUtils.formatWorkOutput(
                    works,
                    results,
                    request=None,
                    dbClient=db_client,
                    formats=None,
                    reader=reader_version,
                ),
                "paging": paging,
                "facets": facets,
                "searchParams": params.to_query_filters(),
            }

            data_block = {"data": search_results, "type": "catalog_search"}

            db_client.closeSession()

            return json.dumps(data_block, default=json_dump_uuid)

        self.model = init_chat_model("gemini-2.5-flash", model_provider="google_genai")
        self.agent = create_react_agent(
            model=self.model,
            tools=[search_catalog, search_item],
            prompt=self.system_prompt,
        )

    @property
    def system_prompt(self):
        return VRA_SYSTEM_PROMPT_V0

    def get_chat_completion(self, messages):
        parsed_messages = self._parse_messages(messages)
        response = self.agent.invoke({"messages": parsed_messages})

        answer = ""
        results = None

        for message in response["messages"]:
            message.pretty_print()

            if isinstance(message, ToolMessage):
                results = json.loads(message.content)

        last_content = response["messages"][-1].content
        if isinstance(last_content, list):
            answer = "".join(
                block["text"]
                for block in last_content
                if isinstance(block, dict) and block.get("type") == "text"
            )
        else:
            answer = str(last_content)

        return {
            "answer": answer,
            "results": results,
            "messages": messages_to_dict(parsed_messages),
        }

    def get_initial_message(self, initial_message_type="vra"):
        if initial_message_type == "item":
            answer = "I can help you find relevant content in this book. Ask me a question, or try the suggestions below."
        else:
            answer = "What research topic can I help you explore today?"
        return {
            "answer": answer,
            "results": None,
            "messages": messages_to_dict([self.system_prompt]),
        }

    def _parse_messages(self, messages):
        parsed_messages = messages_from_dict(messages)

        if not isinstance(parsed_messages[0], SystemMessage):
            parsed_messages.insert(0, self.system_prompt)

        return parsed_messages
