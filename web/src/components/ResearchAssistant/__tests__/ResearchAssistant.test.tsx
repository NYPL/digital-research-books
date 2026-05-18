jest.mock("@nypl/web-reader", () => ({
  addTocToManifest: jest.fn(),
  PdfReader: () => <div data-testid="mock-pdf-reader" />,
}));

jest.mock("../CatalogResults/CatalogResults", () => ({
  __esModule: true,
  default: ({ results }) => (
    <div data-testid="catalog-results">
      {results?.search_params?.query?.[0]?.[1] || "no-query"}
    </div>
  ),
}));

import { fireEvent, screen } from "@testing-library/react";
import { renderWithResearchAssistant as render } from "~/src/__tests__/testUtils/render";
import ResearchAssistant from "../ResearchAssistant";

const mockClearHistory = jest.fn();
const mockSendMessage = jest.fn();
const mockGoToPreviousState = jest.fn();
window.scroll = jest.fn();

const mockUseResearchAssistant = jest.fn();
jest.mock("~/src/context/ResearchAssistantContext", () => ({
  useResearchAssistant: () => mockUseResearchAssistant(),
  ResearchAssistantProvider: ({ children }) => <div>{children}</div>,
}));

describe("ResearchAssistant", () => {
  beforeEach(() => {
    mockUseResearchAssistant.mockClear();
    mockUseResearchAssistant.mockReturnValue({
      messages: [],
      sendMessage: mockSendMessage,
      isLoading: false,
      error: null,
      clearHistory: mockClearHistory,
      showChat: true,
      goToPreviousState: mockGoToPreviousState,
    });
  });

  test("renders the Research Assistant", () => {
    render(<ResearchAssistant />);

    expect(
      screen.getByRole("heading", { name: /research assistant/i })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", { name: /start over/i })
    ).toBeInTheDocument();
    expect(
      screen.getByPlaceholderText(/ask your question.../i)
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /send/i })).toBeInTheDocument();
  });

  test("calls clearHistory when the Clear button is clicked", () => {
    render(<ResearchAssistant />);
    fireEvent.click(screen.getByRole("button", { name: /start over/i }));
    expect(mockClearHistory).toHaveBeenCalledTimes(1);
  });

  test("displays an error message when an error occurs", () => {
    mockUseResearchAssistant.mockReturnValue({
      messages: [],
      sendMessage: jest.fn(),
      isLoading: false,
      error: "Failed to fetch response.",
      clearHistory: jest.fn(),
      showChat: true,
    });

    render(<ResearchAssistant />);
    expect(screen.getAllByText(/failed to fetch response./i)).toHaveLength(2);
  });

  test("sends initial message from sessionStorage on first load", () => {
    const initialMessage = "What is the history of the NYPL?";
    const getItemSpy = jest
      .spyOn(window.sessionStorage.__proto__, "getItem")
      .mockReturnValue(initialMessage);
    const removeItemSpy = jest.spyOn(
      window.sessionStorage.__proto__,
      "removeItem"
    );

    render(<ResearchAssistant />);

    expect(getItemSpy).toHaveBeenCalledWith("researchAssistantInitialMessage");
    expect(mockSendMessage).toHaveBeenCalledWith(initialMessage);
    expect(removeItemSpy).toHaveBeenCalledWith(
      "researchAssistantInitialMessage"
    );

    getItemSpy.mockRestore();
    removeItemSpy.mockRestore();
  });

  describe("render most recent non-empty results", () => {
    beforeEach(() => {
      window.HTMLElement.prototype.scrollIntoView = jest.fn();
    });
    const userMessage = {
      id: "1",
      type: "message",
      content: "Query",
      role: "user",
    };
    const assistantMessage = {
      id: "2",
      content: [
        {
          annotations: [],
          text: "Here is what I found.",
          type: "output_text",
        },
      ],
      role: "assistant",
      type: "message",
    };

    const mockCatalogResults = {
      editions: [],
      search_params: { query: [["keyword", "test"]] },
      paging: {
        currentPage: 1,
        firstPage: 1,
        lastPage: 1,
        nextPage: 1,
        previousPage: 1,
        recordsPerPage: 10,
        totalRecords: 0,
      },
    };

    const mockCatalogResultsAtIndex2 = {
      ...mockCatalogResults,
      search_params: { query: [["keyword", "index-2-results"]] },
    };

    const mockCatalogResultsAtIndex1 = {
      ...mockCatalogResults,
      search_params: { query: [["keyword", "index-1-unique-results"]] },
    };

    test("displays catalog results when results are stored at the current message count index", () => {
      mockUseResearchAssistant.mockReturnValue({
        messages: [userMessage, assistantMessage],
        sendMessage: mockSendMessage,
        isLoading: false,
        error: null,
        clearHistory: mockClearHistory,
        showChat: true,
        goToPreviousState: mockGoToPreviousState,
        historyStack: [],
        results: { 2: mockCatalogResultsAtIndex2 },
      });

      render(<ResearchAssistant />);

      expect(screen.getByTestId("catalog-results")).toBeInTheDocument();
      expect(screen.getByText("index-2-results")).toBeInTheDocument();
    });

    test("falls back to most recent non-empty results when no exact match for current message count", () => {
      mockUseResearchAssistant.mockReturnValue({
        messages: [
          userMessage,
          assistantMessage,
          userMessage,
          assistantMessage,
        ],
        sendMessage: mockSendMessage,
        isLoading: false,
        error: null,
        clearHistory: mockClearHistory,
        showChat: true,
        goToPreviousState: mockGoToPreviousState,
        historyStack: [],
        results: {
          1: mockCatalogResultsAtIndex1,
          2: mockCatalogResultsAtIndex2,
        },
      });

      render(<ResearchAssistant />);

      expect(screen.getByTestId("catalog-results")).toBeInTheDocument();
      expect(screen.getByText("index-2-results")).toBeInTheDocument();
      expect(
        screen.queryByText("index-1-unique-results")
      ).not.toBeInTheDocument();
    });

    test("ignores empty object result entries and falls back to the most recent non-empty result", () => {
      mockUseResearchAssistant.mockReturnValue({
        messages: [
          userMessage,
          assistantMessage,
          userMessage,
          assistantMessage,
        ],
        sendMessage: mockSendMessage,
        isLoading: false,
        error: null,
        clearHistory: mockClearHistory,
        showChat: true,
        goToPreviousState: mockGoToPreviousState,
        historyStack: [],
        results: { 3: {}, 1: mockCatalogResultsAtIndex1 },
      });

      render(<ResearchAssistant />);

      expect(screen.getByTestId("catalog-results")).toBeInTheDocument();
      expect(screen.getByText("index-1-unique-results")).toBeInTheDocument();
      expect(screen.queryByText("index-2-results")).not.toBeInTheDocument();
    });

    test("displays no results message when all result entries are empty objects", () => {
      mockUseResearchAssistant.mockReturnValue({
        messages: [userMessage, assistantMessage],
        sendMessage: mockSendMessage,
        isLoading: false,
        error: null,
        clearHistory: mockClearHistory,
        showChat: true,
        goToPreviousState: mockGoToPreviousState,
        historyStack: [],
        results: { 2: {} },
      });

      render(<ResearchAssistant />);

      expect(screen.queryByTestId("catalog-results")).not.toBeInTheDocument();
      expect(
        screen.getByText(/no results found\. try a different topic\./i)
      ).toBeInTheDocument();
    });
  });
});
