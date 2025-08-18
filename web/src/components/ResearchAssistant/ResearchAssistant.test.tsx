import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import ResearchAssistantWindow from "./ResearchAssistantWindow";
import { Message, MessageType } from "~/src/types/ResearchAssistant";
import MessageBubble from "./MessageBubble";
import ResearchAssistantInput from "./ResearchAssistantInput";
import ResearchAssistant from "./ResearchAssistant";

const mockClearHistory = jest.fn();
const mockSendMessage = jest.fn();

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
        });
    });

    test("renders the Research Assistant", () => {
        render(<ResearchAssistant />);

        expect(
            screen.getByRole("heading", { name: /research assistant/i })
        ).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /clear/i })).toBeInTheDocument();
        expect(
            screen.getByPlaceholderText(/ask your question.../i)
        ).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /send/i })).toBeInTheDocument();
    });

    test("calls clearHistory when the Clear button is clicked", () => {
        render(<ResearchAssistant />);
        fireEvent.click(screen.getByRole("button", { name: /clear chat/i }));
        expect(mockClearHistory).toHaveBeenCalledTimes(1);
    });

    test("displays an error message when an error occurs", () => {
        mockUseResearchAssistant.mockReturnValue({
            messages: [],
            sendMessage: jest.fn(),
            isLoading: false,
            error: "Failed to fetch response.",
            clearHistory: jest.fn(),
        });

        render(<ResearchAssistant />);
        expect(screen.getByText(/failed to fetch response./i)).toBeInTheDocument();
    });

    test("displays the initial empty state message in the window", () => {
        render(<ResearchAssistant />);
        expect(
            screen.getByText(/what research topic would you like to explore?/i)
        ).toBeInTheDocument();
    });
});

describe("ResearchAssistantWindow", () => {
    window.HTMLElement.prototype.scrollIntoView = function() {};
    test('displays "What research topic..." when no messages and not loading', () => {
        render(<ResearchAssistantWindow messages={[]} isLoading={false} />);
        expect(
            screen.getByText(/what research topic would you like to explore?/i)
        ).toBeInTheDocument();
    });

    test("renders MessageBubble components for each message", () => {
        const mockMessages: Message[] = [
            { id: "1", data: { content: "Hello" }, type: MessageType.Human },
            { id: "2", data: { content: "Hi there!" }, type: MessageType.Ai },
        ];
        render(
            <ResearchAssistantWindow messages={mockMessages} isLoading={false} />
        );

        expect(screen.getByText("Hello")).toBeInTheDocument();
        expect(screen.getByText("Hi there!")).toBeInTheDocument();
        expect(screen.queryByText(/start a conversation/i)).not.toBeInTheDocument();
    });

    test("displays loading indicator when isLoading is true", () => {
        render(<ResearchAssistantWindow messages={[]} isLoading={true} />);
        expect(screen.getByText(/assistant thinking.../i)).toBeInTheDocument();
    });
});

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
        expect(
            screen.queryByText("Virtual Research Assistant")
        ).not.toBeInTheDocument();
    });

    test("renders assistant message with correct content, header, and styling", () => {
        const assistantMessage: Message = {
            id: "ai1",
            data: { content: "This is an assistant response." },
            type: MessageType.Ai,
        };
        render(<MessageBubble message={assistantMessage} />);

        const messageElement = screen.getByText("This is an assistant response.");
        expect(messageElement).toBeInTheDocument();
    });
});

describe("ResearchAssistantInput", () => {
    const mockOnSendMessage = jest.fn();

    beforeEach(() => {
        mockOnSendMessage.mockClear();
    });

    test("updates the input value when typed into", () => {
        render(
            <ResearchAssistantInput
                onSendMessage={mockOnSendMessage}
                isDisabled={false}
                messages={[]}
            />
        );
        const inputElement = screen.getByPlaceholderText(
            /ask your question.../i
        ) as HTMLInputElement;

        fireEvent.change(inputElement, { target: { value: "Test message" } });
        expect(inputElement.value).toBe("Test message");
    });

    test("calls onSendMessage with the input text and clears the input on submit", () => {
        render(
            <ResearchAssistantInput
                onSendMessage={mockOnSendMessage}
                isDisabled={false}
                messages={[]}
            />
        );
        const inputElement = screen.getByPlaceholderText(
            /ask your question.../i
        ) as HTMLInputElement;
        const sendButton = screen.getByRole("button", { name: /send/i });

        fireEvent.change(inputElement, { target: { value: "My query" } });
        fireEvent.click(sendButton);

        expect(mockOnSendMessage).toHaveBeenCalledTimes(1);
        expect(mockOnSendMessage).toHaveBeenCalledWith("My query");
        expect(inputElement.value).toBe("");
    });

    test("does not call onSendMessage if input is empty", () => {
        render(
            <ResearchAssistantInput
                onSendMessage={mockOnSendMessage}
                isDisabled={false}
                messages={[]}
            />
        );
        const sendButton = screen.getByRole("button", { name: /send/i });

        fireEvent.click(sendButton);
        expect(mockOnSendMessage).not.toHaveBeenCalled();
    });

    test("disables the input and button when isDisabled is true", () => {
        render(
            <ResearchAssistantInput
                onSendMessage={mockOnSendMessage}
                isDisabled={true}
                messages={[]}
            />
        );
        const inputElement = screen.getByPlaceholderText(
            /assistant is thinking.../i
        ) as HTMLInputElement;
        const sendButton = screen.getByRole("button", { name: /send/i });

        expect(inputElement).toBeDisabled();
        expect(sendButton).toBeDisabled();
    });

    test('shows "Assistant is thinking..." placeholder when disabled', () => {
        render(
            <ResearchAssistantInput
                onSendMessage={mockOnSendMessage}
                isDisabled={true}
                messages={[]}
            />
        );
        expect(
            screen.getByPlaceholderText(/assistant is thinking.../i)
        ).toBeInTheDocument();
        expect(
            screen.queryByPlaceholderText(/ask your question.../i)
        ).not.toBeInTheDocument();
    });

    test("does not call onSendMessage if disabled, even with text", () => {
        render(
            <ResearchAssistantInput
                onSendMessage={mockOnSendMessage}
                isDisabled={true}
                messages={[]}
            />
        );
        const inputElement = screen.getByPlaceholderText(
            /assistant is thinking.../i
        ) as HTMLInputElement;
        const sendButton = screen.getByRole("button", { name: /send/i });

        fireEvent.change(inputElement, { target: { value: "Should not send" } });
        fireEvent.click(sendButton);

        expect(mockOnSendMessage).not.toHaveBeenCalled();
    });
});
