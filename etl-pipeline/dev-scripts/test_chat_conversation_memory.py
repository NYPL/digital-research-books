#!/usr/bin/env python3
"""
Test chat endpoint with a long conversation to monitor memory usage.

This script simulates a multi-turn conversation by:
1. Sending an initial user message
2. Receiving the assistant's response
3. Appending the response to conversation history
4. Sending a new user message with different search topic
5. Repeating for 10 total user messages

Usage:
    # Set required environment variables
    export BASE_URL="http://localhost:5050"
    export VRA_API_KEY="your-api-key"
    export USER="your-username"
    export PASSWORD="your-password"

    # Run the test
    python dev-scripts/test_chat_conversation_memory.py

    # Or with live memory monitoring in another terminal:
    python scripts/monitor_chat_memory_live.py --auto-detect
"""

import os
import sys
import json
import requests
from datetime import datetime


# Search topics for each turn
# 10 turns
SEARCH_TOPICS = [
    "Find books about Detroit motor news and vintage american car culture",
    "Search for books about shipbuilding and maritime history",
    "Find works on ancient Roman architecture and engineering",
    "Look for books about jazz music history in New Orleans",
    "Search for literature on quantum physics and particle theory",
    "Find books about medieval European art and manuscripts",
    "Look for works on climate change and environmental science",
    "Search for books about artificial intelligence and machine learning",
    "Find literature on Renaissance poetry and Shakespeare",
    "Look for books about space exploration and astronomy",
]


def get_env_vars():
    """Get required environment variables."""
    required_vars = ["BASE_URL", "VRA_API_KEY", "USER", "PASSWORD"]
    env_vars = {}

    missing = []
    for var in required_vars:
        value = os.environ.get(var)
        if not value:
            missing.append(var)
        env_vars[var] = value

    if missing:
        print(f"Error: Missing required environment variables: {', '.join(missing)}")
        print("\nPlease set:")
        print("  export BASE_URL='http://localhost:5050'")
        print("  export VRA_API_KEY='your-api-key'")
        print("  export USER='your-username'")
        print("  export PASSWORD='your-password'")
        sys.exit(1)

    return env_vars


def make_chat_request(
    base_url, api_key, user, password, messages, conversation_type="catalogSearch"
):
    """Make a request to the chat API."""
    url = f"{base_url.rstrip('/')}/chat"

    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }

    payload = {
        "conversationType": conversation_type,
        "messages": messages,
    }

    response = requests.post(
        url,
        headers=headers,
        auth=(user, password),
        json=payload,
        timeout=120,  # 2 minute timeout for long searches
    )

    response.raise_for_status()
    return response.json()


def print_separator(char="=", length=80):
    """Print a separator line."""
    print(char * length)


def print_message(message, indent=0):
    """Print a message with formatting."""
    role = message.get("role", "unknown")
    content = message.get("content", "")

    prefix = "  " * indent
    print(f"{prefix}[{role.upper()}]")

    # Truncate long content
    if len(content) > 200:
        content = content[:200] + "..."

    for line in content.split("\n"):
        print(f"{prefix}  {line}")


def main():
    print_separator()
    print("Chat Conversation Memory Load Test")
    print_separator()
    print()

    # Get environment variables
    env = get_env_vars()
    base_url = env["BASE_URL"]
    api_key = env["VRA_API_KEY"]
    user = env["USER"]
    password = env["PASSWORD"]

    print(f"Target: {base_url}")
    print(f"User: {user}")
    print(f"Turns: {len(SEARCH_TOPICS)}")
    print()

    # Initialize conversation
    conversation = []

    # Track statistics
    stats = {
        "turns": 0,
        "total_messages": 0,
        "total_request_size_kb": 0,
        "total_response_size_kb": 0,
        "response_times": [],
    }

    try:
        for turn_num, search_topic in enumerate(SEARCH_TOPICS, 1):
            print_separator("-")
            print(f"Turn {turn_num}/{len(SEARCH_TOPICS)}")
            print(f"Time: {datetime.now().strftime('%H:%M:%S')}")
            print_separator("-")

            # Add user message
            user_message = {"role": "user", "content": search_topic}
            conversation.append(user_message)

            print(f"\nUser Message:")
            print(f"  {search_topic}")
            print(f"\nConversation length: {len(conversation)} messages")

            # Calculate request size
            request_json = json.dumps(
                {"conversationType": "catalogSearch", "messages": conversation}
            )
            request_size_kb = len(request_json.encode("utf-8")) / 1024
            stats["total_request_size_kb"] += request_size_kb
            print(f"Request size: {request_size_kb:.2f} KB")

            # Make API request
            print("\nSending request...")
            start_time = datetime.now()

            try:
                response_data = make_chat_request(
                    base_url=base_url,
                    api_key=api_key,
                    user=user,
                    password=password,
                    messages=conversation,
                    conversation_type="catalogSearch",
                )

                end_time = datetime.now()
                response_time = (end_time - start_time).total_seconds()
                stats["response_times"].append(response_time)

                print(f"✓ Response received in {response_time:.2f}s")

                # Extract data from response (wrapped by formatResponseObject)
                data = response_data.get("data", {})

                # Calculate response size breakdown
                response_json = json.dumps(response_data)
                response_size_kb = len(response_json.encode("utf-8")) / 1024
                stats["total_response_size_kb"] += response_size_kb

                response_messages = data.get("messages", [])
                result = data.get("result")

                messages_size_kb = (
                    len(json.dumps(response_messages).encode("utf-8")) / 1024
                )
                result_size_kb = len(json.dumps(result).encode("utf-8")) / 1024

                print(f"Response size: {response_size_kb:.2f} KB")
                print(
                    f"  ├─ messages ({len(response_messages)} msgs): {messages_size_kb:.2f} KB"
                )
                print(f"  └─ result data:           {result_size_kb:.2f} KB")

                if response_messages:
                    # Append all response messages to conversation
                    conversation.extend(response_messages)
                    stats["total_messages"] += len(response_messages)

                # Show result info if present
                if result:
                    result_type = data.get("result_type")
                    if result_type == "catalogSearch":
                        editions = result.get("editions", [])
                        print(f"\nSearch returned {len(editions)} editions")
                    elif result_type == "contentSearch":
                        snippets = result.get("snippets", [])
                        print(f"\nSearch returned {len(snippets)} snippets")

                stats["turns"] += 1

            except requests.exceptions.RequestException as e:
                print(f"✗ Request failed: {e}")
                if hasattr(e.response, "text"):
                    print(f"Response: {e.response.text[:500]}")
                raise

            print(f"\nTotal conversation length: {len(conversation)} messages")
            print()

        # Print final statistics
        print_separator("=")
        print("Test Complete - Statistics")
        print_separator("=")
        print(f"\nSuccessful turns: {stats['turns']}/{len(SEARCH_TOPICS)}")
        print(
            f"Total messages exchanged: {stats['total_messages'] + len(SEARCH_TOPICS)}"
        )
        print(f"Final conversation length: {len(conversation)} messages")
        print(f"\nData Transfer:")
        print(f"  Total request size: {stats['total_request_size_kb']:.2f} KB")
        print(f"  Total response size: {stats['total_response_size_kb']:.2f} KB")
        print(
            f"  Combined: {stats['total_request_size_kb'] + stats['total_response_size_kb']:.2f} KB"
        )
        print(f"\nResponse Times:")
        print(f"  Min: {min(stats['response_times']):.2f}s")
        print(f"  Max: {max(stats['response_times']):.2f}s")
        print(
            f"  Avg: {sum(stats['response_times']) / len(stats['response_times']):.2f}s"
        )
        print(f"  Total: {sum(stats['response_times']):.2f}s")
        print()

        # Note about memory monitoring
        print("Memory Monitoring:")
        print("  To monitor API memory during this test, run in another terminal:")
        print("  python scripts/monitor_chat_memory_live.py --auto-detect")
        print()
        print_separator("=")

        # Save conversation to file for inspection
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = os.path.join(script_dir, "..", "scratch", "test_conversation_data")
        os.makedirs(output_dir, exist_ok=True)
        output_file = (
            f"conversation_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        output_path = os.path.join(output_dir, output_file)

        with open(output_path, "w") as f:
            json.dump(
                {
                    "conversation": conversation,
                    "stats": stats,
                },
                f,
                indent=2,
            )
        print(f"\n✓ Conversation saved to: {output_path}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Test failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
