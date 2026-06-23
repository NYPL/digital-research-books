import { render, screen } from "@testing-library/react";
import {
  createEdition,
  createItem,
  createWork,
} from "~/src/__tests__/fixtures/CatalogSearchFixture";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import { PageType } from "~/src/types/ResearchAssistant";
import { ResultCard } from "../ResultCard";

const mockEdition = createEdition({
  id: 1,
  title: "Test Title",
  work_title: "Test Title",
  work_uuid: "1234",
  items: [createItem()],
});

const mockAuthors = mockEdition.work_authors;
const mockWork = createWork(mockEdition);

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
