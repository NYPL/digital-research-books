import { screen } from "@testing-library/react";
import {
  catalogResults,
  manyAuthorsCatalogResults,
  minimalCatalogResults,
  singleAuthorCatalogResults,
} from "~/src/__tests__/fixtures/CatalogSearchFixture";
import { renderWithResearchAssistant } from "~/src/__tests__/testUtils/render";
import CatalogResults from "../CatalogResults";

describe("CatalogResults", () => {
  beforeEach(() => {
    (global.fetch as jest.Mock).mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ message: "test" }),
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  test("renders catalog results with all fields", async () => {
    renderWithResearchAssistant(<CatalogResults results={catalogResults} />);

    expect(await screen.findByText("Test Book 1")).toBeInTheDocument();
    expect(screen.getByText(/Author One/)).toBeInTheDocument();
    expect(screen.getByText(/Author Two/)).toBeInTheDocument();
  });

  test("renders multiple catalog items", async () => {
    renderWithResearchAssistant(<CatalogResults results={catalogResults} />);

    expect(await screen.findByText("Test Book 1")).toBeInTheDocument();
    expect(screen.getByText("Test Book 2")).toBeInTheDocument();
  });

  test("handles missing optional fields", async () => {
    renderWithResearchAssistant(
      <CatalogResults results={minimalCatalogResults} />
    );
    expect(await screen.findByText("Minimal Book")).toBeInTheDocument();
  });

  test("renders links for each catalog item", async () => {
    renderWithResearchAssistant(<CatalogResults results={catalogResults} />);

    const workLinks = await screen.findAllByRole("link", {
      name: /Test Book/i,
    });

    expect(workLinks).toHaveLength(2);
    expect(workLinks[0]).toHaveAttribute("href", "/work/1");
    expect(workLinks[1]).toHaveAttribute("href", "/work/2");
  });

  test("renders author links", async () => {
    renderWithResearchAssistant(<CatalogResults results={catalogResults} />);

    const authorLinks = await screen.findAllByRole("link", {
      name: /Author/i,
    });

    expect(authorLinks.length).toBeGreaterThan(0);
    expect(authorLinks[0]).toHaveAttribute(
      "href",
      expect.stringContaining("Author+One")
    );
  });

  test("handles catalog items with single author", async () => {
    renderWithResearchAssistant(
      <CatalogResults results={singleAuthorCatalogResults} />
    );
    expect(await screen.findByText("Solo Author")).toBeInTheDocument();
  });

  test("handles catalog items with many authors", async () => {
    renderWithResearchAssistant(
      <CatalogResults results={manyAuthorsCatalogResults} />
    );
    expect(await screen.findByText(/Author 1/)).toBeInTheDocument();
    expect(screen.getByText(/Author 4/)).toBeInTheDocument();
  });
});
