import { fireEvent, render, screen } from "@testing-library/react";
import RelevantSections from "../RelevantSections";

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
    render(<RelevantSections snippets={mockSnippets} workId="123" />);
  });

  test("should not render if there are no snippets", () => {
    expect(screen.queryByText("Relevant sections")).not.toBeInTheDocument();
  });

  test("should render the component with snippets", () => {
    expect(screen.getByText("View relevant sections")).toBeInTheDocument();
  });

  test("should toggle open and display snippets when 'View relevant sections' is clicked", () => {
    expect(screen.queryByText("Page 5")).not.toBeInTheDocument();
    expect(screen.queryByText("Page 10")).not.toBeInTheDocument();
    fireEvent.click(screen.getByText("View relevant sections"));
    expect(screen.getByText("Relevant sections")).toBeInTheDocument();
    expect(screen.getByText("Page 5")).toBeInTheDocument();
    expect(screen.getByText("Page 10")).toBeInTheDocument();
    expect(
      screen.getByText('"This is a snippet of text."')
    ).toBeInTheDocument();
    expect(screen.getByText('"Another snippet of text."')).toBeInTheDocument();
  });

  test("should toggle closed when 'Hide relevant sections' is clicked", () => {
    fireEvent.click(screen.getByText("View relevant sections"));
    expect(screen.getByText("Page 5")).toBeInTheDocument();
    expect(screen.getByText("Page 10")).toBeInTheDocument();
    fireEvent.click(screen.getByText("Hide relevant sections"));
    expect(screen.queryByText("Page 5")).not.toBeInTheDocument();
    expect(screen.queryByText("Page 10")).not.toBeInTheDocument();
  });

  test("should generate correct links for snippets", () => {
    fireEvent.click(screen.getByText("View relevant sections"));
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
});
