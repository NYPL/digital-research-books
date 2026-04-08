import { fireEvent, screen } from "@testing-library/react";
import { renderWithResearchAssistant } from "~/src/__tests__/testUtils/render";
import RelevantSections from "../RelevantSections";

beforeEach(() => {
  (global.fetch as jest.Mock).mockResolvedValue({
    ok: true,
    json: async () => ({}),
  });
});

describe("RelevantSections", () => {
  const mockSnippets = [
    {
      chunk_score: 0.9,
      start_page: 5,
      end_page: 5,
      item_id: 123,
      text: "This is a snippet of text.",
    },
    {
      chunk_score: 0.8,
      start_page: 10,
      end_page: 10,
      item_id: 456,
      text: "Another snippet of text.",
    },
  ];
  beforeEach(() => {
    renderWithResearchAssistant(
      <RelevantSections snippets={mockSnippets} workId="123" />
    );
  });

  test("should not render if there are no snippets", () => {
    expect(screen.queryByText("Relevant sections")).not.toBeInTheDocument();
  });

  test("renders the component with snippets", () => {
    expect(
      screen.getByText(`View ${mockSnippets.length} relevant sections`)
    ).toBeInTheDocument();
  });

  test("toggles open and display snippets when 'View relevant sections' is clicked", () => {
    expect(screen.queryByText("Page 5")).not.toBeInTheDocument();
    expect(screen.queryByText("Page 10")).not.toBeInTheDocument();
    fireEvent.click(
      screen.getByText(`View ${mockSnippets.length} relevant sections`)
    );
    expect(screen.getByText("Relevant sections")).toBeInTheDocument();
    expect(screen.getByText("Page 5")).toBeInTheDocument();
    expect(screen.getByText("Page 10")).toBeInTheDocument();
    expect(
      screen.getByText('"This is a snippet of text."')
    ).toBeInTheDocument();
    expect(screen.getByText('"Another snippet of text."')).toBeInTheDocument();
  });

  test("toggles closed when 'Hide relevant sections' is clicked", () => {
    fireEvent.click(
      screen.getByText(`View ${mockSnippets.length} relevant sections`)
    );
    expect(screen.getByText("Page 5")).toBeInTheDocument();
    expect(screen.getByText("Page 10")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Hide relevant sections"));
    expect(screen.queryByText("Page 5")).not.toBeInTheDocument();
    expect(screen.queryByText("Page 10")).not.toBeInTheDocument();
  });
});
