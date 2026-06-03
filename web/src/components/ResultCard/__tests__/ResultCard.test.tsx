import { render, screen } from "@testing-library/react";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import { CatalogEdition, PageType } from "~/src/types/ResearchAssistant";
import { ResultCard } from "../ResultCard";

const mockAuthors = [
  {
    lcnaf: "",
    name: "Test Author",
    primary: "true",
    viaf: "",
  },
];
const mockEdition = {
  id: 1,
  title: "Test Title",
  items: [
    {
      content_type: "ebook",
      contributors: [],
      drm: null,
      edition_id: 1,
      id: 2001,
      links: [
        {
          content: null,
          flags: {
            reader: true,
            catalog: false,
            download: false,
          },
          id: 3002,
          media_type: "application/ocr",
          url: "https://example.org/reader/1",
        },
        {
          content: null,
          flags: {
            embed: true,
            catalog: false,
            download: false,
            reader: false,
          },
          id: 3003,
          media_type: "text/html",
          url: "https://example.org/embed/2",
        },
      ],
      measurements: null,
      physical_location: null,
      rights: [
        {
          license: "public_domain",
          rights_statement: "Public Domain",
          source: "test-source",
        },
      ],
      source: "grin",
    },
  ],
  languages: [{ language: "English", iso_2: "en", iso_3: "eng" }],
  links: [],
  measurements: [],
  publication_date: "2024",
  publishers: [],
  snippets: [
    {
      chunk_score: 0.5,
      end_page: 10,
      item_id: 2001,
      start_page: 1,
      text: "Test snippet",
    },
  ],
  work_authors: mockAuthors,
  work_title: "Test Title",
  work_uuid: "1234",
} as CatalogEdition;

const mockWork = {
  uuid: "1234",
  title: "Test Title",
  editions: [mockEdition],
  edition_count: 1,
};

const renderWithPage = (page: PageType) =>
  render(
    <ResultPageProvider value={{ page }}>
      <ResultCard authors={mockAuthors} edition={mockEdition} work={mockWork} />
    </ResultPageProvider>
  );

describe("ResultCard", () => {
  describe("renders book content in card", () => {
    test("renders title with link", () => {
      renderWithPage("vra");
      expect(
        screen.getByRole("link", { name: "Test Title" })
      ).toBeInTheDocument();
    });

    test("renders authors", () => {
      renderWithPage("vra");
      expect(screen.getByText(/Test Author/i)).toBeInTheDocument();
    });

    test("renders edition year", () => {
      renderWithPage("vra");
      expect(screen.getByText(/2024 edition/i)).toBeInTheDocument();
    });

    test("renders relevant sections accordion item", () => {
      renderWithPage("vra");

      const relevanceLabel = screen.getByText(/Why am I seeing this result/i);
      expect(relevanceLabel).toBeInTheDocument();
    });

    test("does not render relevant sections accordion item", () => {
      renderWithPage("keyword");

      expect(
        screen.queryByText(/Why am I seeing this result/i)
      ).not.toBeInTheDocument();
    });
  });
});
