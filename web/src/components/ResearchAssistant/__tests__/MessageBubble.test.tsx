import { render, screen } from "@testing-library/react";
import { Item, ItemType, MessageRole } from "~/src/types/ResearchAssistant";
import MessageBubble from "../MessageBubble";

describe("MessageBubble", () => {
  test("renders user message with correct content and styling", () => {
    const userMessage: Item = {
      type: ItemType.Message,
      content: "This is a user message.",
      role: MessageRole.User,
    };
    render(<MessageBubble index={0} message={userMessage} />);

    const messageElement = screen.getByText("This is a user message.");
    expect(messageElement).toBeInTheDocument();
    expect(messageElement.closest("div")).toHaveClass("userBubble");
  });

  test("renders assistant message with correct content", () => {
    const assistantMessage: Item = {
      type: ItemType.Message,
      content: [
        {
          annotations: [],
          text: "This is an assistant response.",
          type: "output_text",
        },
      ],
      role: MessageRole.Assistant,
    };
    render(<MessageBubble index={0} message={assistantMessage} />);

    expect(
      screen.getByText("This is an assistant response.")
    ).toBeInTheDocument();
    expect(screen.getByText("VRA:")).toBeInTheDocument();
  });

  test("renders AI generated text indicator for non-initial assistant messages", () => {
    const assistantMessage: Item = {
      type: ItemType.Message,
      content: [{ annotations: [], text: "Response", type: "output_text" }],
      role: MessageRole.Assistant,
    };
    render(<MessageBubble index={0} message={assistantMessage} />);

    expect(screen.getByText(/AI-generated/i)).toBeInTheDocument();
  });

  test("renders feedback buttons for non-initial assistant messages", () => {
    const assistantMessage: Item = {
      type: ItemType.Message,
      content: [{ annotations: [], text: "Response", type: "output_text" }],
      role: MessageRole.Assistant,
    };
    render(<MessageBubble index={1} message={assistantMessage} />);

    expect(
      screen.getByRole("button", { name: /thumbs up/i })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /thumbs down/i })
    ).toBeInTheDocument();
  });

  test("does not render feedback buttons for initial assistant message", () => {
    const initialMessage: Item = {
      type: ItemType.Message,
      content: [
        { annotations: [], text: "Initial message", type: "output_text" },
      ],
      role: MessageRole.Assistant,
    };
    render(<MessageBubble index={0} message={initialMessage} />);

    expect(
      screen.queryByRole("button", { name: /thumbs up/i })
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /thumbs down/i })
    ).not.toBeInTheDocument();
  });

  test("renders VRA label and icon for assistant messages", () => {
    const assistantMessage: Item = {
      type: ItemType.Message,
      content: [
        { annotations: [], text: "Message with icon", type: "output_text" },
      ],
      role: MessageRole.Assistant,
    };
    render(<MessageBubble index={1} message={assistantMessage} />);

    const icons = screen.getAllByRole("img", { hidden: true });
    expect(screen.getByText("VRA:")).toBeInTheDocument();
    expect(icons.length).toBeGreaterThan(0);
  });

  test("handles message with special characters", () => {
    const specialMessage: Item = {
      type: ItemType.Message,
      content: "Test <script>alert('xss')</script> & symbols",
      role: MessageRole.User,
    };
    render(<MessageBubble index={0} message={specialMessage} />);

    expect(screen.getByText(/Test.*symbols/)).toBeInTheDocument();
  });
});
