import { DSProvider } from "@nypl/design-system-react-components";
import { fireEvent, screen } from "@testing-library/react";
import mockRouter from "next-router-mock";
import React from "react";
import { render } from "~/src/__tests__/testUtils/render";
import ResearchAssistantLanding from "./ResearchAssistantLanding";

describe("ResearchAssistantLanding", () => {
  const renderWithDSProvider = (ui: React.ReactElement) => {
    return render(<DSProvider>{ui}</DSProvider>);
  };

  beforeEach(() => {
    mockRouter.setCurrentUrl("/research-assistant-landing");
    renderWithDSProvider(<ResearchAssistantLanding />);
  });

  test("renders the ResearchAssistantLanding component", () => {
    expect(
      screen.getByRole("heading", {
        name: /New! The NYPL Virtual Research Assistant/i,
      })
    ).toBeInTheDocument();
    expect(
      screen.getByText(/Your AI partner in discovering content from over/i)
    ).toBeInTheDocument();
    expect(
      screen.getByPlaceholderText(
        /what research topic can i help you explore today?/i
      )
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /send/i })).toBeInTheDocument();
  });

  test("navigates to /research-assistant when search is submitted", () => {
    const inputElement = screen.getByPlaceholderText(
      /what research topic can i help you explore today?/i
    ) as HTMLInputElement;
    const sendButton = screen.getByRole("button", { name: /send/i });

    fireEvent.change(inputElement, { target: { value: "ancient Egypt" } });
    fireEvent.click(sendButton);
    expect(mockRouter.asPath).toBe("/research-assistant");
  });

  test("navigates to /research-assistant when a suggestion is clicked", () => {
    const suggestionButton = screen.getByText(/the science of shipbuilding/i);

    fireEvent.click(suggestionButton);

    expect(mockRouter.asPath).toBe("/research-assistant");
  });

  test("does not navigate if the query is empty", () => {
    const sendButton = screen.getByRole("button", { name: /send/i });

    fireEvent.click(sendButton);
    expect(mockRouter.asPath).toBe("/research-assistant-landing");
  });
});
