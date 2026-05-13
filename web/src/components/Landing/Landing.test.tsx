import { screen, within } from "@testing-library/react";
import {
  searchFormRenderTests,
  searchFormTests,
} from "../../__tests__/componentHelpers/SearchForm";
import LandingPage from "./Landing";

import mockRouter from "next-router-mock";
import {
  collectionListData,
  collections,
} from "~/src/__tests__/fixtures/CollectionFixture";
import { render } from "~/src/__tests__/testUtils/render";
import DrbBreakout from "../DrbBreakout/DrbBreakout";

describe("Renders Index Page", () => {
  beforeEach(async () => {
    render(<LandingPage collections={collectionListData.collections} />);

    // Wait for page to be loaded
    await screen.findByRole("heading", {
      name: "Digitized Research Books",
    });
  });
  test("Current page breadcrumb doesn't have href attribute", () => {
    render(<DrbBreakout />);

    const breadcrumbNavs = screen.getAllByRole("navigation", {
      name: /breadcrumb/i,
    });

    const breadcrumbItem = within(breadcrumbNavs[0]).getByText(
      "Digitized Research Books"
    );

    expect(breadcrumbItem).not.toHaveAttribute("href");
  });
  test("Shows Heading", () => {
    expect(
      screen.getByRole("heading", { name: "Digitized Research Books" })
    ).toBeInTheDocument();
  });

  describe("Landing Page Searchbar", () => {
    searchFormRenderTests();
  });

  test("Shows Recently Added Collections", () => {
    expect(screen.getByText("Recently Added Collections")).toBeInTheDocument();
    collections.forEach((collection) => {
      expect(screen.getByText(collection.title)).toBeInTheDocument();
      expect(screen.getByText(collection.title).closest("a").href).toContain(
        collection.href
      );
    });
  });
});

describe("Search using Landing Page Searchbar", () => {
  beforeEach(async () => {
    render(<LandingPage />);
    // Wait for page to be loaded
    await screen.findByRole("heading", {
      name: "Digitized Research Books",
    });
  });
  searchFormTests(mockRouter);
});
