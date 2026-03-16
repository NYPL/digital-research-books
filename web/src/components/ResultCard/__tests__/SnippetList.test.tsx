import { fireEvent, render, screen, within } from "@testing-library/react";
import SnippetList from "../SnippetList";

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

const manySnippets = Array.from({ length: 7 }, (_, i) => ({
  chunk_score: 0.5,
  start_page: i + 1,
  end_page: i + 1,
  item_id: 100 + i,
  text: `Snippet ${i + 1}`,
}));

describe("SnippetList", () => {
  test("renders snippet links and text", () => {
    render(<SnippetList snippets={mockSnippets} workId="123" />);
    expect(screen.getByText("Page 5")).toBeInTheDocument();
    expect(screen.getByText("Page 10")).toBeInTheDocument();
    expect(
      screen.getByText('"This is a snippet of text."')
    ).toBeInTheDocument();
    expect(screen.getByText('"Another snippet of text."')).toBeInTheDocument();
  });

  test("generates correct links for snippets", () => {
    render(<SnippetList snippets={mockSnippets} workId="123" />);
    const links = screen.getAllByRole("link");
    expect(links).toHaveLength(2);
    expect(links[0]).toHaveAttribute(
      "href",
      "/item/123?previewItemId=123&previewPage=00000005"
    );
    expect(links[1]).toHaveAttribute(
      "href",
      "/item/123?previewItemId=456&previewPage=00000010"
    );
  });

  test("renders pagination if more than 6 snippets", () => {
    render(<SnippetList snippets={manySnippets} workId="123" />);
    expect(screen.getByRole("navigation")).toBeInTheDocument();
  });

  test("focuses first snippet link on page change", () => {
    render(<SnippetList snippets={manySnippets} workId="123" />);
    const pagination = screen.getByRole("navigation");
    fireEvent.click(within(pagination).getByRole("link", { name: "Page 2" }));
    const firstSnippet = screen.getByText("Page 7");
    expect(firstSnippet).toBeInTheDocument();
    const firstSnippetLink = firstSnippet.closest("a");
    expect(firstSnippetLink).toHaveFocus();
  });
});
