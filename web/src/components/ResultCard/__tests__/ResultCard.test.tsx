import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import {
  createEdition,
  createItem,
  createWork,
} from "~/src/__tests__/fixtures/CatalogSearchFixture";
import { FeedbackProvider } from "~/src/context/FeedbackContext";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import { PageType } from "~/src/types/ResearchAssistant";
import { ResultCard } from "../ResultCard";

const mockEdition = createEdition({
  id: 1,
  title: "Test Title",
  work_title: "Test Title",
  work_uuid: "1234",
  items: [createItem()],
  snippets: [
    {
      chunk_score: 0.9,
      start_page: 1,
      end_page: 1,
      item_id: 2001,
      text: "Snippet text",
    },
  ],
});

const mockAuthors = mockEdition.work_authors;
const mockWork = createWork(mockEdition);

type EditionWithCallId = ReturnType<typeof createEdition> & {
  call_id: string;
};

const editionForReasonTest: EditionWithCallId = {
  ...mockEdition,
  call_id: "test-call-id-1",
};

const authorsForReasonTest = editionForReasonTest.work_authors;
const workForReasonTest = createWork(editionForReasonTest);

const renderWithPage = (page: PageType) =>
  render(
    <FeedbackProvider>
      <ResultPageProvider value={{ page }}>
        <ResultCard
          authors={mockAuthors}
          edition={mockEdition}
          work={mockWork}
        />
      </ResultPageProvider>
    </FeedbackProvider>
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

    test("renders relevant information in Why am I seeing this?", async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          explanation:
            "This result is relevant because it matches your prompt.",
        }),
      });

      render(
        <FeedbackProvider>
          <ResultPageProvider value={{ page: "vra" }}>
            <ResultCard
              authors={authorsForReasonTest}
              edition={editionForReasonTest}
              work={workForReasonTest}
            />
          </ResultPageProvider>
        </FeedbackProvider>
      );

      fireEvent.click(
        screen.getByRole("button", { name: /why am i seeing this result/i })
      );

      await waitFor(() => {
        expect(global.fetch).toHaveBeenCalledTimes(1);
      });

      expect(global.fetch).toHaveBeenCalledWith(
        "/api/result-reason",
        expect.objectContaining({
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            call_id: "test-call-id-1",
            edition_id: 1,
          }),
        })
      );

      expect(
        await screen.findByText(
          /This result is relevant because it matches your prompt/i
        )
      ).toBeInTheDocument();
    });

    test("display error message when result reason API fails.", async () => {
      (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: false,
        json: async () => ({
          error: "error",
        }),
      });

      render(
        <FeedbackProvider>
          <ResultPageProvider value={{ page: "vra" }}>
            <ResultCard
              authors={authorsForReasonTest}
              edition={editionForReasonTest}
              work={workForReasonTest}
            />
          </ResultPageProvider>
        </FeedbackProvider>
      );

      fireEvent.click(
        screen.getByRole("button", { name: /why am i seeing this result/i })
      );

      await waitFor(() => {
        expect(global.fetch).toHaveBeenCalledTimes(1);
      });

      expect(global.fetch).toHaveBeenCalledWith(
        "/api/result-reason",
        expect.objectContaining({
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            call_id: "test-call-id-1",
            edition_id: 1,
          }),
        })
      );

      expect(
        await screen.findByText(/Content could not be generated at this time/i)
      ).toBeInTheDocument();
    });

    test("does not render relevant sections accordion item", () => {
      renderWithPage("keyword");

      expect(
        screen.queryByText(/Why am I seeing this result/i)
      ).not.toBeInTheDocument();
    });
  });
});
