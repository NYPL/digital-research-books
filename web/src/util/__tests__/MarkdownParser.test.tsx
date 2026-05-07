import { fireEvent, screen } from "@testing-library/react";
import { render } from "~/src/__tests__/testUtils/render";
import { renderMarkdownContent } from "~/src/util/MarkdownParser";

describe("renderMarkdownContent", () => {
  test("renders markdown ordered lists and bold text", () => {
    const markdown =
      "Summary text.\n\n1. **First point** with details\n\n2. **Second point** with details";

    const { container } = render(
      <div>{renderMarkdownContent(markdown, jest.fn())}</div>
    );

    expect(screen.getByText("Summary text.")).toBeInTheDocument();
    expect(screen.getByText("First point")).toBeInTheDocument();
    expect(screen.getByText("Second point")).toBeInTheDocument();
    expect(container).not.toHaveTextContent(/\*\*First point\*\*/);
    expect(container).not.toHaveTextContent(/\*\*Second point\*\*/);

    expect(container.querySelectorAll("ol")).toHaveLength(1);
    expect(screen.getAllByRole("listitem")).toHaveLength(2);
  });

  test("renders edition tags as clickable links", () => {
    const onEditionClick = jest.fn();
    const markdown =
      'Reference (<edition id="editionId-1234">The unrivaled history of the world</edition>, page 303).';

    render(<div>{renderMarkdownContent(markdown, onEditionClick)}</div>);

    const editionLink = screen.getByRole("link", {
      name: "The unrivaled history of the world",
    });

    fireEvent.click(editionLink);

    expect(onEditionClick).toHaveBeenCalledWith("editionId-1234");
  });

  test("renders page references as links to related edition item pages", () => {
    const markdown =
      'Reference (<edition id="editionId-1234">A history of English literature</edition>, page 389).\n\n* Rejection of innate ideas (page 389).\n* Explicit reference (edition_15280000, page 410).';

    render(<div>{renderMarkdownContent(markdown, jest.fn())}</div>);

    const implicitPageLinks = screen.getAllByRole("link", {
      name: "(page 389)",
    });
    expect(implicitPageLinks).toHaveLength(1);
    expect(implicitPageLinks[0]).toHaveAttribute(
      "href",
      "/item/workId-1234?previewItemId=itemId-1234&previewPage=00000389"
    );
    const explicitPageLink = screen.getByRole("link", {
      name: "(edition_editionId-1234, page 410)",
    });
    expect(explicitPageLink).toHaveAttribute(
      "href",
      "/item/15280000?previewItemId=15280000&previewPage=00000410"
    );
  });

  test("uses work id in page reference path when resolver provides one", () => {
    const markdown =
      'Reference <edition id="15279445">A history of English literature</edition> (page 389).';

    const resolveWorkIdByEditionId = (editionId: string) =>
      editionId === "15279445"
        ? { workId: "8842001", itemId: "99001" }
        : undefined;

    render(
      <div>
        {renderMarkdownContent(markdown, jest.fn(), resolveWorkIdByEditionId)}
      </div>
    );

    const implicitPageLink = screen.getByRole("link", {
      name: "(page 389)",
    });
    expect(implicitPageLink).toHaveAttribute(
      "href",
      "/item/8842001?previewItemId=99001&previewPage=00000389"
    );
  });

  test("renders edition tag nested inside bold as a bold clickable link", () => {
    const onEditionClick = jest.fn();
    const markdown =
      '* **<edition id="15269461">History of Ottoman Turks</edition> by Edward Shepherd Creasy (1854)**: Details here.';

    render(<div>{renderMarkdownContent(markdown, onEditionClick)}</div>);

    const editionLink = screen.getByRole("link", {
      name: "History of Ottoman Turks",
    });

    // The link should be present and clickable
    fireEvent.click(editionLink);
    expect(onEditionClick).toHaveBeenCalledWith("15269461");

    // The surrounding text should also be rendered (not swallowed)
    expect(screen.getByText(/by Edward Shepherd Creasy/)).toBeInTheDocument();
  });

  test("renders links and bold-wrapped links", () => {
    const markdown =
      "See [Google](https://google.com) and **[Bold Link](https://example.com)** for details.";

    render(<div>{renderMarkdownContent(markdown, jest.fn())}</div>);

    const googleLink = screen.getByRole("link", { name: "Google" });
    expect(googleLink).toHaveAttribute("href", "https://google.com");

    const boldLink = screen.getByRole("link", { name: "Bold Link" });
    expect(boldLink).toHaveAttribute("href", "https://example.com");
  });

  test("renders markdown headings and keeps related list sections", () => {
    const markdown =
      "Intro paragraph.\n\n### Key Historical Periods\n1. **Foundation** details\n2. **Consolidation** details\n\n### Governance and Society\n* **The Sultan** details";

    const { container } = render(
      <div>{renderMarkdownContent(markdown, jest.fn())}</div>
    );

    expect(screen.getAllByRole("heading", { level: 3 })).toHaveLength(2);
    expect(screen.getByText("Key Historical Periods")).toBeInTheDocument();
    expect(screen.getByText("Governance and Society")).toBeInTheDocument();

    expect(container.querySelectorAll("ol")).toHaveLength(1);
    expect(container.querySelectorAll("ul")).toHaveLength(1);
    expect(screen.getByText("Foundation")).toBeInTheDocument();
    expect(screen.getByText("The Sultan")).toBeInTheDocument();
  });
});
