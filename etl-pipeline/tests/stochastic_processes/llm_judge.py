"""
Shared LLM-as-judge helper for stochastic process tests.
"""

import json
from typing import Literal

from openai import AsyncOpenAI
from pydantic import BaseModel

from agents.items import TResponseInputItem

from utils.common import require_env


class JudgeVerdict(BaseModel):
    reason: str
    answer: Literal["YES", "NO"]


def extract_final_output(messages: list[TResponseInputItem]) -> str:
    """Extract the assistant's final response text.

    The last item must be a completed assistant message; anything else means
    the conversation didn't actually end in a response to the user.
    """
    assert messages, "messages is empty, no final output to extract"
    final_item = messages[-1]
    assert (
        final_item.get("type") == "message" and final_item.get("role") == "assistant"
    ), f"Expected final item to be an assistant message, got: {final_item}"
    return "".join(
        part["text"]
        for part in final_item["content"]
        if part.get("type") == "output_text"
    )


async def llm_judge(messages: list[TResponseInputItem], question: str) -> JudgeVerdict:
    """
    Run an LLM-as-judge evaluation over the full conversation history.

    The judge receives the serialized conversation (`messages`, i.e. input
    items as returned by RunResult.to_input_list()) embedded in the system
    prompt, then answers a YES/NO question about it. `default=str` handles
    non-serializable items in the history (e.g. datetime).
    """
    client = AsyncOpenAI(
        api_key=require_env("GOOGLE_API_KEY"),
        base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    )
    conversation_json = json.dumps(messages, indent=2, default=str)

    response = await client.chat.completions.parse(
        model="gemini-3.1-pro-preview",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an impartial evaluator judging an AI assistant's "
                    "conversation. You are given the full conversation history below, "
                    "including tool call inputs and outputs. Answer the evaluation "
                    "question with YES or NO and explain your reasoning.\n\n"
                    f"CONVERSATION:\n{conversation_json}"
                ),
            },
            {
                "role": "user",
                "content": f"EVALUATION QUESTION: {question}",
            },
        ],
        response_format=JudgeVerdict,
        temperature=0,
    )
    parsed = response.choices[0].message.parsed
    assert parsed is not None, "LLM judge returned a null response (possible refusal)"

    print("QUESTION:", question)
    print("AGENT FINAL RESPONSE:", extract_final_output(messages))
    print("VERDICT:", parsed.answer)
    print("REASON:", parsed.reason)
    return parsed
