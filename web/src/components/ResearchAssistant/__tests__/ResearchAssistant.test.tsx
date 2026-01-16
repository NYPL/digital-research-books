import { fireEvent, render, screen } from "@testing-library/react";
import ResearchAssistant from "../ResearchAssistant";

const mockClearHistory = jest.fn();
const mockSendMessage = jest.fn();
const mockGoToPreviousState = jest.fn();

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
    expect(screen.getByText(/failed to fetch response./i)).toBeInTheDocument();
  });

  test("renders BackToResultsButton when there are results and history", () => {
    mockUseResearchAssistant.mockReturnValue({
      ...mockUseResearchAssistant(),
      results: { type: "catalog_search", data: [] },
      historyStack: [{}, {}],
    });

    render(<ResearchAssistant />);

    expect(
      screen.getByRole("button", { name: /back to results/i })
    ).toBeInTheDocument();
  });

  test("calls goToPreviousState when BackToResultsButton is clicked", () => {
    mockUseResearchAssistant.mockReturnValue({
      ...mockUseResearchAssistant(),
      results: { type: "catalog_search", data: [] },
      historyStack: [{}, {}],
    });

    render(<ResearchAssistant />);

    const backButton = screen.getByRole("button", { name: /back to results/i });
    fireEvent.click(backButton);

    expect(mockGoToPreviousState).toHaveBeenCalledTimes(1);
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
});
