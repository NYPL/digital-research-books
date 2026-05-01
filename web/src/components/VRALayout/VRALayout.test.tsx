import { screen } from "@testing-library/react";
import { renderWithResearchAssistant } from "../../__tests__/testUtils/render";
import VRALayout from "./VRALayout";

describe("VRA Layout component", () => {
  beforeEach(() => {
    renderWithResearchAssistant(
      <VRALayout activePage="vra">
        <div>Text in layout body</div>
      </VRALayout>
    );
  });

  test("Digital Research Books Beta doesn't have href attribute", () => {
    const homepagelinks = screen.getAllByText("Digital Research Books Beta");
    homepagelinks.forEach((link) => {
      expect(link).not.toHaveAttribute("href");
    });
  });
  test("DRB Header is shown", () => {
    expect(
      screen.getByRole("heading", { name: "Digital Research Books Beta" })
    ).toBeInTheDocument();
  });
  test("should have text in layout body", () => {
    const text = screen.getByText("Text in layout body");
    expect(text).toBeInTheDocument();
  });
});
