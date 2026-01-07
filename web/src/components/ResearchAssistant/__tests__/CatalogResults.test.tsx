import { fireEvent, screen, waitFor } from "@testing-library/react";
import {
  catalogResults,
  emptyCatalogResults,
  manyAuthorsCatalogResults,
  minimalCatalogResults,
  multiPageCatalogResults,
  singleAuthorCatalogResults,
} from "~/src/__tests__/fixtures/CatalogSearchFixture";
import { renderWithResearchAssistant } from "~/src/__tests__/testUtils/render";
import { searchResultsFetcher } from "~/src/lib/api/SearchApi";
import CatalogResults from "../CatalogResults";

jest.mock("~/src/lib/api/SearchApi");
const mockedSearchResultsFetcher = jest.mocked(searchResultsFetcher);

const mockSetViewState = jest.fn();

const mockUseResearchAssistant = jest.fn();
jest.mock("~/src/context/ResearchAssistantContext", () => ({
  useResearchAssistant: () => mockUseResearchAssistant(),
  ResearchAssistantProvider: ({ children }) => <div>{children}</div>,
}));

describe("CatalogResults", () => {
  beforeEach(() => {
    mockUseResearchAssistant.mockClear();
    mockUseResearchAssistant.mockReturnValue({
      setViewState: mockSetViewState,
      messages: [],
      sendMessage: jest.fn(),
      isLoading: false,
      error: null,
      clearHistory: jest.fn(),
      showChat: true,
      results: null,
      historyStack: [],
      goToPreviousState: jest.fn(),
    });
    mockedSearchResultsFetcher.mockResolvedValue({
      data: { works: [], totalWorks: 0, paging: { lastPage: 1 } },
    } as any);
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

  test('renders "No results" message when results are empty', async () => {
    renderWithResearchAssistant(
      <CatalogResults results={emptyCatalogResults} />
    );

    expect(
      await screen.findByText(/No results matching your research criteria/i)
    ).toBeInTheDocument();
  });

  test("calls the search API and updates state on page change", async () => {
    renderWithResearchAssistant(
      <CatalogResults results={multiPageCatalogResults} />
    );

    const nextButton = screen.getByRole("link", { name: /next page/i });
    fireEvent.click(nextButton);

    await waitFor(() => {
      expect(mockedSearchResultsFetcher).toHaveBeenCalledWith(
        expect.objectContaining({
          page: 2,
          query: "keyword:test",
        })
      );
    });

    await waitFor(() => {
      expect(mockSetViewState).toHaveBeenCalledTimes(1);
    });
  });
});
