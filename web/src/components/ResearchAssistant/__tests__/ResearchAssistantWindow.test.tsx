import { render, screen } from "@testing-library/react";
import { Item, ItemType, MessageRole } from "~/src/types/ResearchAssistant";
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
    const mockMessages: Item[] = [
      {
        id: "1",
        content: "Hello",
        role: MessageRole.User,
        type: ItemType.Message,
      },
      {
        id: "2",
        content: [{ annotations: [], text: "Hi there!", type: "output_text" }],
        role: MessageRole.Assistant,
        type: ItemType.Message,
      },
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
    const mockMessages: Item[] = [
      {
        id: "1",
        content: "Question",
        role: MessageRole.User,
        type: ItemType.Message,
      },
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
    const mockMessages: Item[] = [
      {
        id: "1",
        content: "First message",
        role: MessageRole.User,
        type: ItemType.Message,
      },
      {
        id: "2",
        content: [
          { annotations: [], text: "Second message", type: "output_text" },
        ],
        role: MessageRole.Assistant,
        type: ItemType.Message,
      },
      {
        id: "3",
        content: "Third message",
        role: MessageRole.User,
        type: ItemType.Message,
      },
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
