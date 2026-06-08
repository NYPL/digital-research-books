import { render, screen } from "@testing-library/react";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import { PageType } from "~/src/types/ResearchAssistant";
import OcrReadLink from "../Ctas/OcrReadLink";

const readLink = {
  link_id: 1,
  mediaType: "text/html",
  url: "preview",
  flags: {
    catalog: false,
    download: false,
    reader: false,
  },
};

const workId = "work1";
const editionId = 123;
const title = "Test Book";

const renderWithProvider = (page: PageType) =>
  render(
    <ResultPageProvider value={{ page }}>
      <OcrReadLink
        readLink={readLink}
        workId={workId}
        editionId={editionId}
        title={title}
      />
    </ResultPageProvider>
  );

describe("EnhancedSearchReadLink", () => {
  test("renders preview button in VRA context", () => {
    renderWithProvider("vra");
    expect(
      screen.getByRole("link", { name: /Read online/i })
    ).toBeInTheDocument();
  });

  test("renders not available if not VRA context", () => {
    renderWithProvider("keyword");
    expect(screen.getByText(/Not yet available/i)).toBeInTheDocument();
  });
});
