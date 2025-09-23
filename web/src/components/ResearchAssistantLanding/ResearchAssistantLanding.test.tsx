import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import mockRouter from "next-router-mock";
import ResearchAssistantLanding from "./ResearchAssistantLanding";

describe("ResearchAssistantLanding", () => {
    beforeEach(() => {
        mockRouter.setCurrentUrl("/research-assistant-landing");
    });

    test("renders the ResearchAssistantLanding component", () => {
        render(<ResearchAssistantLanding />);
        expect(
            screen.getByRole("heading", {
                name: /introducing the nypl virtual research assistant/i,
            })
        ).toBeInTheDocument();
        expect(
            screen.getByText(/your ai partner in discovering relevant research/i)
        ).toBeInTheDocument();
        expect(
            screen.getByPlaceholderText(
                /what research topic can i help you explore today?/i
            )
        ).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /send/i })).toBeInTheDocument();
    });

    test("navigates to /research-assistant when search is submitted", () => {
        render(<ResearchAssistantLanding />);
        const inputElement = screen.getByPlaceholderText(
            /what research topic can i help you explore today?/i
        ) as HTMLInputElement;
        const sendButton = screen.getByRole("button", { name: /send/i });

        fireEvent.change(inputElement, { target: { value: "ancient Egypt" } });
        fireEvent.click(sendButton);
        expect(mockRouter.asPath).toBe("/research-assistant");
    });

    test("navigates to /research-assistant when a suggestion is clicked", () => {
        render(<ResearchAssistantLanding />);
        const suggestionButton = screen.getByText(
            /show me books on feminism in medieval times/i
        );

        fireEvent.click(suggestionButton);

        expect(mockRouter.asPath).toBe("/research-assistant");
    });

    test("does not navigate if the query is empty", () => {
        render(<ResearchAssistantLanding />);
        const sendButton = screen.getByRole("button", { name: /send/i });

        fireEvent.click(sendButton);
        expect(mockRouter.asPath).toBe("/research-assistant-landing");
    });
});
