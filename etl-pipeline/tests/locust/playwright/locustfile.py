"""
Locust-Playwright load test that simulates a full end-user journey:
landing page → catalog search → optional follow-up → item page → content search

The following events are reported:
- Landing page navigation (landing page: load)
- Chat panel response from a catalog search (results page: chat response)
- Chat panel response from a catalog search follow-up (results page: chat response follow-up)
- Item page navigation (item page: load)
- Chat panel response from a content search (item page: chat response)

Event names are kept static for clean aggregation. The active prompt is logged before each
event block and embedded in failure exception messages for per-prompt attribution.

Example usage:
    from etl-pipeline/:
        VRA_USER_AUTH_TOKEN=insert_token_here \
        uv run \
        --with locust \
        --with "locust-plugins[playwright]" \
        locust -f tests/locust/playwright/locustfile.py \
        --host https://drb-qa.nypl.org \
        --users 5 \
        --spawn-rate 0.0167 \
        --run-time 10m
"""

import asyncio
import json
import logging
import os
import random
import re

from locust import between, task
from locust_plugins.users.playwright import PlaywrightUser, event, pw
from playwright.async_api import Page

logger = logging.getLogger(__name__)

TIMEOUT_MS = 120_000  # 2 min

CATALOG_SEARCH_PROMPTS = [
    "What period in the last 1000 years had the most religious strife?",
    "Theories of planetary motion before Newton",
    "The development of calculus and the Newton-Leibniz priority dispute",
    "Notable women writers in Victorian England",
    "The development of polyphony in medieval church music",
    "Theories of value and labor in 19th century economic thought",
    "Notable women in Polynesian culture",
    "Epistemological debates between rationalism and empiricism in the 17th century",
    "Heretical movements within medieval Christianity",
    "European cartographic traditions before the Age of Exploration",
    "Symbolism in Gothic cathedral architecture",
]

SUGGESTION_PROMPTS = [
    "Political figures of Ancient Rome",
    "Medieval warfare in China",
    # "Ornithology in the nineteenth century",  # Omitted (SCHOL-588)
    "The science of shipbuilding",
]

FOLLOW_UP_PROMPTS = [
    "Show me titles published only before 1900",  # Date filter
    "Limit these to books written in English",  # Language filter
    "Narrow these down to non-fiction or historical works",  # Subject filter
    "Which of these would be best for a graduate student?",  # Conversational
]

BOOK_PROMPTS = [
    "What time period does this book focus on?",  # Date info
    "What are the main topics of this book?",  # Subject info
]


class VRAUser(PlaywrightUser):
    wait_time = between(30, 60)  # Break before each user restarts the journey

    async def log_latest_chat_bubble_text(self, bubbles, label: str) -> None:
        """Log the inner text of the most-recently-appeared VRA chat bubble."""
        count = await bubbles.count()
        if count:
            text = await bubbles.nth(count - 1).evaluate(
                "el => el.parentElement?.innerText ?? el.innerText"
            )
            logger.info("[%s] response=%r", label, text.strip())

    @task
    @pw
    async def enhanced_search_full_journey(self, page: Page):
        """Simulates a complete user journey from landing page through item page."""
        vra_chat_bubbles = page.get_by_text("VRA:", exact=True)
        search_results = page.get_by_test_id("search-result")
        chat_input = page.get_by_role("textbox", name="Ask your question...")
        send_button = page.get_by_role("button", name="Send").first

        page.set_default_timeout(TIMEOUT_MS)
        page.set_default_navigation_timeout(TIMEOUT_MS)

        # 0. Authorize by setting token in localStorage to bypass login
        user_auth_token = os.environ["VRA_USER_AUTH_TOKEN"]
        await page.context.add_init_script(
            f"localStorage.setItem('authToken', {json.dumps(user_auth_token)})"
        )

        # 1. Navigate to landing page
        async with event(self, "landing page: load"):
            await page.goto("/research-assistant-landing")
            await page.get_by_role(
                "heading", level=1, name=re.compile("NYPL Virtual Research Assistant")
            ).wait_for()

        await asyncio.sleep(random.uniform(3, 8))  # Choose text entry or suggestion

        # 2. Submit prompt via textbox or suggestion button (no events)
        if random.random() > 0.5:
            prompt = random.choice(CATALOG_SEARCH_PROMPTS)
            await page.get_by_placeholder("What research topic").fill(prompt)
            await send_button.click()
        else:
            prompt = random.choice(SUGGESTION_PROMPTS)
            await page.get_by_role("button", name=prompt).click()

        # 3. Wait for chat panel response and at least one search result on results page
        logger.info("[catalog search] submitting prompt='%s'", prompt)
        async with event(self, "results page: chat response"):
            try:
                await page.wait_for_url(re.compile(r".+/research-assistant$"))
                await vra_chat_bubbles.nth(1).wait_for()
            except Exception as e:
                raise Exception(f"[{prompt}] {type(e).__name__}: {e}") from e
        await self.log_latest_chat_bubble_text(vra_chat_bubbles, "catalog search")
        try:
            await search_results.first.wait_for()
        except Exception as e:
            raise Exception(
                f"[{prompt}] No search results returned: {type(e).__name__}: {e}"
            ) from e

        await asyncio.sleep(random.uniform(8, 20))  # Look through results list

        # NOTE: Omitted (SCHOL-593)
        # 4. Optionally submit a follow-up prompt on results page
        # if random.random() > 0.5:
        #     follow_up = random.choice(FOLLOW_UP_PROMPTS)
        #     logger.info("[catalog search] submitting follow-up prompt='%s'", follow_up)
        #     await chat_input.fill(follow_up)
        #     await send_button.click()
        #     async with event(self, "results page: chat response follow-up"):
        #         try:
        #             await vra_chat_bubbles.nth(2).wait_for()
        #         except Exception as e:
        #             raise Exception(
        #                 f"[{follow_up}] No chat response returned: {type(e).__name__}: {e}"
        #             ) from e
        #     await self.log_latest_chat_bubble_text(
        #         vra_chat_bubbles, "catalog search follow-up"
        #     )
        #     try:
        #         await search_results.first.wait_for()
        #     except Exception as e:
        #         raise Exception(
        #             f"[{follow_up}] No search results returned: {type(e).__name__}: {e}"
        #         ) from e

        # 5. Navigate to item page for a search result
        await search_results.first.wait_for()
        result_count = await search_results.count()
        selected_result = search_results.nth(random.randint(0, result_count - 1))
        edition_div_id = await selected_result.locator(
            "[id^='edition-']"
        ).first.get_attribute("id")
        edition_id = (
            edition_div_id.removeprefix("edition-") if edition_div_id else "unknown"
        )
        logger.info("[item page] navigating to edition %s", edition_id)
        async with event(self, "item page: load"):
            await (
                selected_result.get_by_test_id("result-title")
                .get_by_role("link")
                .click()
            )
            await page.wait_for_url(re.compile(r".*/item/.*"))
            await page.get_by_role("heading", level=1).nth(1).wait_for()  # Book title

        await asyncio.sleep(random.uniform(10, 30))  # Digest content on page

        # 6. Submit a prompt about the book
        book_prompt = random.choice(BOOK_PROMPTS)
        logger.info("[content search] submitting prompt='%s'", book_prompt)
        async with event(self, "item page: chat response"):
            try:
                vra_chat_bubble_count_before = await vra_chat_bubbles.count()
                await chat_input.wait_for(state="visible")
                await chat_input.fill(book_prompt)
                await send_button.click()
                await vra_chat_bubbles.nth(vra_chat_bubble_count_before).wait_for()
            except Exception as e:
                raise Exception(
                    f"[{book_prompt}] edition_id={edition_id} {type(e).__name__}: {e}"
                ) from e
        await self.log_latest_chat_bubble_text(vra_chat_bubbles, "content search")

        await asyncio.sleep(random.uniform(5, 10))  # Read response and end journey
