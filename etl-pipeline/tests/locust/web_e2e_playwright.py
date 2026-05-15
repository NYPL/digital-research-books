"""
Locust-Playwright load test that simulates a full end-user journey:
landing page → catalog search → optional follow-up → item page → content search

The following events are reported:
- Landing page navigation (landing page: load)
- Chat panel response from a catalog search (results page: chat response)
- Chat panel response from a catalog search follow-up (results page: chat response follow-up)
- Item page navigation (item page: load)
- Chat panel response from a content search (item page: chat response)

Event names are kept static for clean aggregation. The active prompt is logged before
each event block and embedded in failure exception messages for per-prompt attribution.

This script is designed to be run locally, in CI, or with LoadForge.

Example usage (local/CI):
    from etl-pipeline/:
        VRA_USER_AUTH_TOKEN=insert_token_here \
        uv run \
          --with "locust" \
          --with "locust-plugins[playwright]" \
          locust \
            -f tests/locust/web_e2e_playwright.py \
            --host <localhost|qa|etc> \
            --users 5 \
            --run-time 10m \
            --spawn-rate 0.0167

This will start a localhost server at port 8089. Navigate there in a browser to
access the web UI for starting the test and viewing results, logs, etc. in real-time.
- Specifying host, users, spawn rate, and run time just autofills the web UI form.
- Adding --headless bypasses the web UI and runs the test immediately in the terminal.
  - All test run parameters must be specified on the command line in this case.

NOTE:
The spawn rate above is set such that max concurrency is reached at the halfway point of
the test run. This is a standard approach used for ramping-up users, but faster or
slower ramp-ups may be used to meet different test criteria (e.g. DoS testing).
- The float value is determined by dividing the number of users by half the test run
  duration in seconds: (5 users / (600 seconds / 2)) = 0.0167 users per second
"""

import asyncio
import json
import os
import pathlib
import random
import re
import socket

from locust import between, task
from locust_plugins.users.playwright import PlaywrightUser, event, pw
from playwright.async_api import Page

UI_TIMEOUT_MS = 60_000  # 60s for navigation and UI interactions
CHAT_TIMEOUT_MS = 120_000  # 120s for chat responses

_IS_LOADFORGE_RUN = socket.gethostname().startswith("loadforge-")

# Load test user prompts from external file
# For LoadForge, prompts are stored in their internal file system
if _IS_LOADFORGE_RUN:
    try:
        with open("files/enhanced_search_user_prompts.json", "r") as f:
            prompts = json.load(f)
    except Exception as e:
        raise FileNotFoundError(
            f"Error loading user prompts file: {type(e).__name__}: {e}"
        ) from e
else:
    USER_PROMPTS_FILE = (
        pathlib.Path(__file__).parent.parent / "fixtures" / "user_prompts.json"
    )
    prompts = json.loads(USER_PROMPTS_FILE.read_text())

CATALOG_SEARCH_PROMPTS: list[str] = prompts["catalog_search"]
SUGGESTION_PROMPTS: list[str] = prompts["suggestion"]
FOLLOW_UP_PROMPTS: list[str] = prompts["follow_up"]
BOOK_SEARCH_PROMPTS: list[str] = prompts["book_search"]


class EnhancedSearchWebUser(PlaywrightUser):
    wait_time = between(30, 60)  # Break before each user restarts the journey

    async def log_latest_chat_bubble_text(self, bubbles, label: str) -> None:
        """Log the inner text of the most-recently-appeared VRA chat bubble."""
        count = await bubbles.count()
        if count:
            text = await bubbles.nth(count - 1).evaluate(
                "el => el.parentElement?.innerText ?? el.innerText"
            )
            print(f"[{label}] response={json.dumps(text.strip())}")

    @task
    @pw
    async def enhanced_search_full_journey(self, page: Page):
        """Simulates a complete user journey from landing page through item page."""
        chat_input = page.locator("#chat-input")
        search_results = page.get_by_test_id("search-result")
        send_button = page.get_by_role("button", name="Send").first
        vra_chat_bubbles = page.get_by_text("VRA:", exact=True)

        page.set_default_timeout(UI_TIMEOUT_MS)
        page.set_default_navigation_timeout(UI_TIMEOUT_MS)

        # 0. Authorize by setting token in localStorage to bypass login
        if _IS_LOADFORGE_RUN:
            try:
                with open("files/authToken-vratestuser.txt", "r") as f:
                    user_auth_token = f.readline().strip()
            except Exception as e:
                raise FileNotFoundError(
                    f"Error loading file containing authToken: {type(e).__name__}: {e}"
                ) from e
        else:
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
            await chat_input.fill(prompt)
            await send_button.click()
        else:
            prompt = random.choice(SUGGESTION_PROMPTS)
            await page.get_by_role("button", name=prompt).click()

        # 3. Wait for chat panel response and at least one search result on results page
        print(f"[catalog search] submitting prompt='{prompt}'")
        async with event(self, "results page: chat response"):
            try:
                await page.wait_for_url(re.compile(r".+/research-assistant$"))
                await vra_chat_bubbles.nth(1).wait_for(timeout=CHAT_TIMEOUT_MS)
            except Exception as e:
                raise Exception(f"[{prompt}] {type(e).__name__}: {e}") from e
        await self.log_latest_chat_bubble_text(vra_chat_bubbles, "catalog search")
        try:
            await search_results.first.wait_for(timeout=5000)  # Chat req finished
        except Exception as e:
            raise Exception(
                f"[{prompt}] No search results returned: {type(e).__name__}: {e}"
            ) from e

        await asyncio.sleep(random.uniform(8, 20))  # Look through results list

        # NOTE: Omitted (SCHOL-593)
        # 4. Optionally submit a follow-up prompt on results page
        # if random.random() > 0.5:
        #     follow_up = random.choice(FOLLOW_UP_PROMPTS)
        #     print(f"[catalog search] submitting follow-up prompt='{follow_up}'")
        #     await chat_input.fill(follow_up)
        #     await send_button.click()
        #     async with event(self, "results page: chat response follow-up"):
        #         try:
        #             await vra_chat_bubbles.nth(2).wait_for(timeout=CHAT_TIMEOUT_MS)
        #         except Exception as e:
        #             raise Exception(
        #                 f"[{follow_up}] No chat response returned: {type(e).__name__}: {e}"
        #             ) from e
        #     await self.log_latest_chat_bubble_text(
        #         vra_chat_bubbles, "catalog search follow-up"
        #     )
        #     try:
        #         await search_results.first.wait_for(timeout=5000)  # Chat req finished
        #     except Exception as e:
        #         raise Exception(
        #             f"[{follow_up}] No search results returned: {type(e).__name__}: {e}"
        #         ) from e

        # 5. Navigate to item page for a search result
        result_count = await search_results.count()
        selected_result = search_results.nth(random.randint(0, result_count - 1))
        edition_div_id = await selected_result.locator(
            "[id^='edition-']"
        ).first.get_attribute("id")
        edition_id = (
            edition_div_id.removeprefix("edition-") if edition_div_id else "unknown"
        )
        print(f"[item page] navigating to edition {edition_id}")
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
        book_prompt = random.choice(BOOK_SEARCH_PROMPTS)
        print(f"[content search] submitting prompt='{book_prompt}'")
        async with event(self, "item page: chat response"):
            try:
                await vra_chat_bubbles.nth(0).wait_for()
                await chat_input.wait_for(state="visible")
                await chat_input.fill(book_prompt)
                await send_button.click()
                await vra_chat_bubbles.nth(1).wait_for(timeout=CHAT_TIMEOUT_MS)
            except Exception as e:
                raise Exception(
                    f"[{book_prompt}] edition_id={edition_id} {type(e).__name__}: {e}"
                ) from e
        await self.log_latest_chat_bubble_text(vra_chat_bubbles, "content search")

        await asyncio.sleep(random.uniform(5, 10))  # Read response and end journey


# disable_random_checks
