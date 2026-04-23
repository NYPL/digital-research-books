import { screen } from "@testing-library/react";
import { render } from "~/src/__tests__/testUtils/render";
import ResearchAssistantNav from "../ResearchAssistantNav";

describe("ResearchAssistantNav", () => {
  test("renders navigation tabs with correct hrefs", () => {
    render(<ResearchAssistantNav activePage={"vra"} />);

    const navItems = screen.getAllByRole("link");
    const keywordSearchTab = screen.getByText(/keyword search/i);
    const vraTab = screen.getByText(/virtual research assistant/i);
    expect(navItems.length).toBeGreaterThanOrEqual(2);
    expect(vraTab).toBeInTheDocument();
    expect(vraTab.closest("a")).toHaveAttribute(
      "href",
      "/research-assistant-landing"
    );
    expect(keywordSearchTab).toBeInTheDocument();
    expect(keywordSearchTab.closest("a")).toHaveAttribute(
      "href",
      "/keyword-search-landing"
    );
  });

  test("highlights VRA tab when active", () => {
    render(<ResearchAssistantNav activePage={"vra"} />);

    const vraTab = screen.getByText(/virtual research assistant/i);
    const keywordTab = screen.getByText(/keyword search/i);
    expect(vraTab.closest("a")).toHaveAttribute("aria-current", "page");
    expect(keywordTab.closest("a")).not.toHaveAttribute("aria-current", "page");
  });

  test("highlights keyword search tab when active", () => {
    render(<ResearchAssistantNav activePage={"keyword"} />);

    const keywordTab = screen.getByText(/keyword search/i);
    const vraTab = screen.getByText(/virtual research assistant/i);
    expect(keywordTab.closest("a")).toHaveAttribute("aria-current", "page");
    expect(vraTab.closest("a")).not.toHaveAttribute("aria-current", "page");
  });

  test("displays beta badge on VRA tab", () => {
    render(<ResearchAssistantNav activePage={"vra"} />);

    expect(screen.getByText(/beta/i)).toBeInTheDocument();
  });
});
