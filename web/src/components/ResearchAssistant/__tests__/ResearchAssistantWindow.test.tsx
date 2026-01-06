import { render, screen } from "@testing-library/react";
import { Message, MessageType } from "~/src/types/ResearchAssistant";
import ResearchAssistantWindow from "../ResearchAssistantWindow";

const mockUseResearchAssistant = jest.fn();
window.scroll = jest.fn();

jest.mock("~/src/context/ResearchAssistantContext", () => ({
  useResearchAssistant: () => mockUseResearchAssistant(),
}));

describe("ResearchAssistantWindow", () => {
  window.HTMLElement.prototype.scrollIntoView = function () {};

  beforeEach(() => {
    mockUseResearchAssistant.mockClear();
  });

  test("renders MessageBubble components for each message", () => {
    const mockMessages: Message[] = [
      { id: "1", data: { content: "Hello" }, type: MessageType.Human },
      { id: "2", data: { content: "Hi there!" }, type: MessageType.Ai },
    ];
    mockUseResearchAssistant.mockReturnValue({
      messages: mockMessages,
      isLoading: false,
    });

    render(<ResearchAssistantWindow />);

    expect(screen.getByText("Hello")).toBeInTheDocument();
    expect(screen.getByText("Hi there!")).toBeInTheDocument();
  });

  test("displays loading indicator when isLoading is true", () => {
    mockUseResearchAssistant.mockReturnValue({
      messages: [],
      isLoading: true,
    });

    render(<ResearchAssistantWindow />);

    expect(screen.getByText(/assistant thinking.../i)).toBeInTheDocument();
  });

  test("displays loading indicator with existing messages", () => {
    const mockMessages: Message[] = [
      { id: "1", data: { content: "Question" }, type: MessageType.Human },
    ];
    mockUseResearchAssistant.mockReturnValue({
      messages: mockMessages,
      isLoading: true,
    });

    render(<ResearchAssistantWindow />);

    expect(screen.getByText("Question")).toBeInTheDocument();
    expect(screen.getByText(/assistant thinking.../i)).toBeInTheDocument();
  });

  test("renders messages in correct order", () => {
    const mockMessages: Message[] = [
      { id: "1", data: { content: "First message" }, type: MessageType.Human },
      { id: "2", data: { content: "Second message" }, type: MessageType.Ai },
      { id: "3", data: { content: "Third message" }, type: MessageType.Human },
    ];
    mockUseResearchAssistant.mockReturnValue({
      messages: mockMessages,
      isLoading: false,
    });

    render(<ResearchAssistantWindow />);

    const messages = screen.getAllByText(/message/i);
    expect(messages[0]).toHaveTextContent("First message");
    expect(messages[1]).toHaveTextContent("Second message");
    expect(messages[2]).toHaveTextContent("Third message");
  });
});
