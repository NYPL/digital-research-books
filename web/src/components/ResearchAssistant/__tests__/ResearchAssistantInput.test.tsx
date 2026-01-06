import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import ResearchAssistantInput from "../ResearchAssistantInput";

const mockSendMessage = jest.fn();
const mockUseResearchAssistant = jest.fn();

jest.mock("~/src/context/ResearchAssistantContext", () => ({
  ...jest.requireActual("~/src/context/ResearchAssistantContext"),
  useResearchAssistant: () => mockUseResearchAssistant(),
}));

describe("ResearchAssistantInput", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseResearchAssistant.mockReturnValue({
      messages: [],
      sendMessage: mockSendMessage,
      isLoading: false,
    });
  });

  test("renders input and send button", () => {
    render(<ResearchAssistantInput />);

    expect(
      screen.getByPlaceholderText(/ask your question.../i)
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /send/i })).toBeInTheDocument();
  });

  test("updates the input value when typed into", async () => {
    const user = userEvent.setup();
    render(<ResearchAssistantInput />);

    const inputElement = screen.getByPlaceholderText(
      /ask your question.../i
    ) as HTMLTextAreaElement;

    await user.type(inputElement, "Test message");
    expect(inputElement.value).toBe("Test message");
  });

  test("calls sendMessage with the input text and clears the input on button click", async () => {
    render(<ResearchAssistantInput />);

    const inputElement = screen.getByPlaceholderText(
      /ask your question.../i
    ) as HTMLTextAreaElement;
    const sendButton = screen.getByRole("button", { name: /send/i });

    fireEvent.change(inputElement, { target: { value: "My query" } });
    fireEvent.click(sendButton);

    await waitFor(() => {
      expect(mockSendMessage).toHaveBeenCalledTimes(1);
    });

    expect(inputElement.value).toBe("");
  });

  test("calls sendMessage on Enter key press", async () => {
    render(<ResearchAssistantInput />);

    const inputElement = screen.getByPlaceholderText(
      /ask your question.../i
    ) as HTMLTextAreaElement;

    fireEvent.change(inputElement, { target: { value: "Enter query" } });
    fireEvent.keyDown(inputElement, { key: "Enter", code: "Enter" });

    await waitFor(() => {
      expect(mockSendMessage).toHaveBeenCalledTimes(1);
    });

    expect(inputElement.value).toBe("");
  });

  test("does not call sendMessage on Shift+Enter", () => {
    render(<ResearchAssistantInput />);

    const inputElement = screen.getByPlaceholderText(
      /ask your question.../i
    ) as HTMLTextAreaElement;

    fireEvent.change(inputElement, { target: { value: "Shift Enter query" } });
    fireEvent.keyDown(inputElement, {
      key: "Enter",
      code: "Enter",
      shiftKey: true,
    });

    expect(mockSendMessage).not.toHaveBeenCalled();
  });

  test("does not call sendMessage if input is empty", () => {
    render(<ResearchAssistantInput />);

    const sendButton = screen.getByRole("button", { name: /send/i });

    fireEvent.click(sendButton);
    expect(mockSendMessage).not.toHaveBeenCalled();
  });

  test("does not call sendMessage if input contains only whitespace", () => {
    render(<ResearchAssistantInput />);

    const inputElement = screen.getByPlaceholderText(
      /ask your question.../i
    ) as HTMLTextAreaElement;
    const sendButton = screen.getByRole("button", { name: /send/i });

    fireEvent.change(inputElement, { target: { value: "   " } });
    fireEvent.click(sendButton);

    expect(mockSendMessage).not.toHaveBeenCalled();
  });

  test("disables the input and button when isLoading is true", () => {
    mockUseResearchAssistant.mockReturnValue({
      messages: [],
      sendMessage: mockSendMessage,
      isLoading: true,
    });

    render(<ResearchAssistantInput />);

    const inputElement = screen.getByPlaceholderText(
      /assistant is thinking.../i
    ) as HTMLTextAreaElement;
    const sendButton = screen.getByRole("button", { name: /send/i });

    expect(inputElement).toBeDisabled();
    expect(sendButton).toBeDisabled();
  });

  test('renders "Assistant is thinking..." placeholder when disabled', () => {
    mockUseResearchAssistant.mockReturnValue({
      messages: [],
      sendMessage: mockSendMessage,
      isLoading: true,
    });

    render(<ResearchAssistantInput />);

    expect(
      screen.getByPlaceholderText(/assistant is thinking.../i)
    ).toBeInTheDocument();
    expect(
      screen.queryByPlaceholderText(/ask your question.../i)
    ).not.toBeInTheDocument();
  });

  test("does not call sendMessage if disabled", () => {
    mockUseResearchAssistant.mockReturnValue({
      messages: [],
      sendMessage: mockSendMessage,
      isLoading: true,
    });

    render(<ResearchAssistantInput />);

    const sendButton = screen.getByRole("button", { name: /send/i });

    fireEvent.click(sendButton);

    expect(mockSendMessage).not.toHaveBeenCalled();
  });

  test("send button is disabled when input is empty", () => {
    render(<ResearchAssistantInput />);

    const sendButton = screen.getByRole("button", { name: /send/i });
    expect(sendButton).toBeDisabled();
  });

  test("send button is enabled when input has text", () => {
    render(<ResearchAssistantInput />);

    const inputElement = screen.getByPlaceholderText(
      /ask your question.../i
    ) as HTMLTextAreaElement;
    const sendButton = screen.getByRole("button", { name: /send/i });

    fireEvent.change(inputElement, { target: { value: "Some text" } });
    expect(sendButton).toBeEnabled();
  });
});
