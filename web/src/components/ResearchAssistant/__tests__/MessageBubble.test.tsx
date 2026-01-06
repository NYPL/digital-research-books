import { render, screen } from "@testing-library/react";
import { Message, MessageType } from "~/src/types/ResearchAssistant";
import MessageBubble from "../MessageBubble";

describe("MessageBubble", () => {
  test("renders user message with correct content and styling", () => {
    const userMessage: Message = {
      id: "user1",
      data: { content: "This is a user message." },
      type: MessageType.Human,
    };
    render(<MessageBubble message={userMessage} />);

    const messageElement = screen.getByText("This is a user message.");
    expect(messageElement).toBeInTheDocument();
    expect(messageElement.closest("div")).toHaveClass("userBubble");
  });

  test("renders assistant message with correct content", () => {
    const assistantMessage: Message = {
      id: "ai1",
      data: { content: "This is an assistant response." },
      type: MessageType.Ai,
    };
    render(<MessageBubble message={assistantMessage} />);

    expect(
      screen.getByText("This is an assistant response.")
    ).toBeInTheDocument();
    expect(screen.getByText("VRA:")).toBeInTheDocument();
  });

  test("renders AI generated text indicator for non-initial assistant messages", () => {
    const assistantMessage: Message = {
      id: "ai2",
      data: { content: "Response" },
      type: MessageType.Ai,
    };
    render(<MessageBubble message={assistantMessage} />);

    expect(screen.getByText(/AI-generated/i)).toBeInTheDocument();
  });

  test("renders feedback buttons for non-initial assistant messages", () => {
    const assistantMessage: Message = {
      id: "ai3",
      data: { content: "Response" },
      type: MessageType.Ai,
    };
    render(<MessageBubble message={assistantMessage} />);

    expect(
      screen.getByRole("button", { name: /thumbs up/i })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /thumbs down/i })
    ).toBeInTheDocument();
  });

  test("does not render feedback buttons for initial assistant message", () => {
    const initialMessage: Message = {
      id: "assistant-initial",
      data: { content: "Initial message" },
      type: MessageType.Ai,
    };
    render(<MessageBubble message={initialMessage} />);

    expect(
      screen.queryByRole("button", { name: /thumbs up/i })
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /thumbs down/i })
    ).not.toBeInTheDocument();
  });

  test("renders VRA label and icon for assistant messages", () => {
    const assistantMessage: Message = {
      id: "ai4",
      data: { content: "Message with icon" },
      type: MessageType.Ai,
    };
    render(<MessageBubble message={assistantMessage} />);

    const icons = screen.getAllByRole("img", { hidden: true });
    expect(screen.getByText("VRA:")).toBeInTheDocument();
    expect(icons.length).toBeGreaterThan(0);
  });

  test("handles message with special characters", () => {
    const specialMessage: Message = {
      id: "special1",
      data: { content: "Test <script>alert('xss')</script> & symbols" },
      type: MessageType.Human,
    };
    render(<MessageBubble message={specialMessage} />);

    expect(screen.getByText(/Test.*symbols/)).toBeInTheDocument();
  });
});
