import { screen } from "@testing-library/react";
import { render } from "~/src/__tests__/testUtils/render";
import {
  ContentSearchResults,
  Item,
  ItemType,
  MessageRole,
} from "~/src/types/ResearchAssistant";
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

    expect(
      screen.getByText(/Thinking... This may take several seconds./i)
    ).toBeInTheDocument();
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
    expect(
      screen.getByText(/Thinking... This may take several seconds./i)
    ).toBeInTheDocument();
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

  describe("relevant snippet display based on message index", () => {
    test("renders relevant snippets for the assistant message using the matching message index", () => {
      const mockSnippet = {
        chunk_score: 0.9,
        start_page: 5,
        end_page: 5,
        item_id: 123,
        text: "relevant snippet text for index test",
      };

      const contentSearchResults: ContentSearchResults = {
        snippets: [mockSnippet],
        search_params: { query: [["keyword", "test"]] },
      };

      const mockMessages: Item[] = [
        {
          id: "1",
          content: "What is relevant?",
          role: MessageRole.User,
          type: ItemType.Message,
        },
        {
          id: "2",
          content: [
            {
              annotations: [],
              text: "Here is what I found.",
              type: "output_text",
            },
          ],
          role: MessageRole.Assistant,
          type: ItemType.Message,
        },
      ];

      mockUseResearchAssistant.mockReturnValue({
        messages: mockMessages,
        isLoading: false,
        results: { 1: contentSearchResults },
        handlePreview: jest.fn(),
      });

      render(<ResearchAssistantWindow />);

      expect(
        screen.getByText(/relevant snippet text for index test/i)
      ).toBeInTheDocument();
    });

    test("displays snippets only for the message with matching index, not from other indices", () => {
      const snippetAtIndex1 = {
        chunk_score: 0.9,
        start_page: 10,
        end_page: 10,
        item_id: 100,
        text: "snippets from index 1",
      };

      const snippetAtIndex3 = {
        chunk_score: 0.85,
        start_page: 20,
        end_page: 20,
        item_id: 200,
        text: "snippets from index 3",
      };

      const resultsAtIndex1: ContentSearchResults = {
        snippets: [snippetAtIndex1],
        search_params: { query: [["keyword", "query1"]] },
      };

      const resultsAtIndex3: ContentSearchResults = {
        snippets: [snippetAtIndex3],
        search_params: { query: [["keyword", "query3"]] },
      };

      const mockMessages: Item[] = [
        {
          id: "1",
          content: "First question",
          role: MessageRole.User,
          type: ItemType.Message,
        },
        {
          id: "2",
          content: [
            {
              annotations: [],
              text: "First response",
              type: "output_text",
            },
          ],
          role: MessageRole.Assistant,
          type: ItemType.Message,
        },
        {
          id: "3",
          content: "Second question",
          role: MessageRole.User,
          type: ItemType.Message,
        },
        {
          id: "4",
          content: [
            {
              annotations: [],
              text: "Second response",
              type: "output_text",
            },
          ],
          role: MessageRole.Assistant,
          type: ItemType.Message,
        },
      ];

      mockUseResearchAssistant.mockReturnValue({
        messages: mockMessages,
        isLoading: false,
        results: { 1: resultsAtIndex1, 3: resultsAtIndex3 },
        handlePreview: jest.fn(),
      });

      render(<ResearchAssistantWindow />);

      expect(screen.getByText(/snippets from index 1/i)).toBeInTheDocument();
      expect(screen.getByText(/snippets from index 3/i)).toBeInTheDocument();
    });

    test("does not display snippets when results are undefined for a message", () => {
      const mockMessages: Item[] = [
        {
          id: "1",
          content: "What is this?",
          role: MessageRole.User,
          type: ItemType.Message,
        },
        {
          id: "2",
          content: [
            {
              annotations: [],
              text: "I found nothing.",
              type: "output_text",
            },
          ],
          role: MessageRole.Assistant,
          type: ItemType.Message,
        },
      ];

      mockUseResearchAssistant.mockReturnValue({
        messages: mockMessages,
        isLoading: false,
        results: { 1: null },
        handlePreview: jest.fn(),
      });

      render(<ResearchAssistantWindow />);

      expect(screen.getByText("I found nothing.")).toBeInTheDocument();
      expect(screen.queryByText(/page \d+/i)).not.toBeInTheDocument();
    });
  });
});
