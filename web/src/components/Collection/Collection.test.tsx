import React from "react";
import { screen } from "@testing-library/react";
import { render } from "../../__tests__/testUtils/render";
import Collection from "./Collection";
import { CollectionQuery, CollectionResult } from "~/src/types/CollectionQuery";
import {
  collectionData,
  collectionWithPagination,
  emptyCollectionResult,
} from "~/src/__tests__/fixtures/CollectionFixture";
import mockRouter from "next-router-mock";
import userEvent from "@testing-library/user-event";

const collectionResults: CollectionResult = collectionData;
const collectionQuery: CollectionQuery = {
  identifier: "id",
  page: 1,
  perPage: 5,
  sort: "relevance",
};

describe("Renders Collection Page", () => {
  beforeEach(() => {
    render(
      <Collection
        collectionQuery={collectionQuery}
        collectionResult={collectionResults}
      />
    );
  });
  test("Shows the current collection with title", () => {
    expect(
      screen.getByText("Collection - Baseball: A Collection by Mike Benowitz")
    ).toBeInTheDocument();
  });
  test("Shows Item Count correctly", () => {
    expect(screen.getByText("Viewing 1 - 3 of 3 items")).toBeInTheDocument();
  });
  describe("Pagination does not appear in collections with <10 items", () => {
    test("Pagination does not appear", () => {
      const previousLink = screen.queryByRole("link", {
        name: "Previous page",
      });
      const nextLink = screen.queryByRole("link", {
        name: "Next page",
      });

      expect(previousLink).not.toBeInTheDocument();
      expect(nextLink).not.toBeInTheDocument();
    });
  });
  describe("Sorts filters", () => {
    test("Changing sort by sends new request", async () => {
      const sortBy = screen.getByLabelText("Sort By");
      expect(sortBy).toBeVisible();
      await userEvent.selectOptions(sortBy, "Title A-Z");
      expect(sortBy).toHaveValue("Title A-Z");
      expect(mockRouter).toMatchObject({
        pathname: "/collection/id",
        query: {
          sort: "title",
        },
      });
    });
  });
  describe("Renders Collection Items", () => {
    test("Title links to edition page", () => {
      expect(
        screen.getByText("Judge Landis and twenty-five years of baseball")
      ).toBeInTheDocument();
      expect(
        screen
          .getByText("Judge Landis and twenty-five years of baseball")
          .closest("a").href
      ).toContain("https://drb-qa.nypl.org/edition/894734");
    });
    test("Author links to author search", () => {
      expect(
        screen.getByText("Wray, J. E. (J. Edward)").closest("a").href
      ).toContain(
        "http://localhost/search?query=author%3AWray%2C+J.+E.+%28J.+Edward%29"
      );
    });

    test("Shows Year as Link in header", () => {
      expect(screen.getByText("1900 Edition").closest("a").href).toContain(
        "https://drb-qa.nypl.org/edition/4267756"
      );
    });
    test("Shows Full Publisher", () => {
      expect(
        screen.getByText(
          "Published in New York (State) by American Sports Publishing,"
        )
      ).toBeInTheDocument();
    });
    test("Shows Full list of languages", () => {
      expect(screen.getByText("Languages: eng,und")).toBeInTheDocument();
    });
    test("Shows license with links", () => {
      expect(
        screen.getByText("Copyright: Public Domain").closest("a").href
      ).toContain("/copyright");
    });
  });
});

describe("Render Collection Page with >10 items", () => {
  beforeEach(() => {
    render(
      <Collection
        collectionQuery={collectionQuery}
        collectionResult={collectionWithPagination}
      />
    );
  });

  describe("Pagination appears", () => {
    test("Previous page link is disabled", () => {
      const previousLink = screen.queryByRole("link", {
        name: "Previous page",
      });
      expect(previousLink).toHaveAttribute("aria-disabled", "true");;
    });
    test("Next page link appears and is clickable", async () => {
      const nextLink = screen.getByRole("link", { name: "Next page" });
      expect(nextLink).toBeInTheDocument();
      await userEvent.click(nextLink);
      expect(mockRouter).toMatchObject({
        pathname: "/collection/id",
        query: {
          page: 2,
        },
      });
    });
    test("Middle numbers are clickable", async () => {
      const twoButton = screen.getByRole("link", { name: "Page 2" });
      expect(twoButton).toBeInTheDocument();
      await userEvent.click(twoButton);
      expect(mockRouter).toMatchObject({
        pathname: "/collection/id",
        query: {
          page: 2,
        },
      });
    });
  });

  describe("Sorts filters", () => {
    test("Changing number of items sends new request", async () => {
      const itemsPerPage = screen.getByLabelText("Items Per Page");
      expect(itemsPerPage).toBeVisible();
      await userEvent.selectOptions(itemsPerPage, "50");
      expect(itemsPerPage).toHaveValue("50");
      expect(mockRouter).toMatchObject({
        pathname: "/collection/id",
        query: {
          perPage: "50",
        },
      });

      // pagination should not show since <50 items
      const previousLink = screen.queryByRole("link", {
        name: "Previous page",
      });
      const nextLink = screen.queryByRole("link", {
        name: "Next page",
      });

      expect(previousLink).not.toBeInTheDocument();
      expect(nextLink).not.toBeInTheDocument();
    });
  });
});

describe("Renders No Results when no results are shown", () => {
  beforeEach(() => {
    render(
      <Collection
        collectionQuery={collectionQuery}
        collectionResult={emptyCollectionResult}
      />
    );
  });

  test("Shows the current collection with title", () => {
    expect(
      screen.getByText("Collection - Baseball: A Collection by Mike Benowitz")
    ).toBeInTheDocument();
  });
  test("Item Count shows correctly", () => {
    expect(screen.getByText("Viewing 0 items")).toBeInTheDocument();
  });
});
