import React from "react";
import { act, cleanup, screen, within } from "@testing-library/react";
import KeywordSearch from "./KeywordSearch";
import { FilterYearsTests } from "./SearchFilters/FilterYearsTests";
import userEvent from "@testing-library/user-event";
import { FilterLanguagesCommonTests } from "./SearchFilters/FilterLanguagesTests";
import mockRouter from "next-router-mock";
import { searchResults } from "~/src/__tests__/fixtures/SearchResultFixture";
import { ApiSearchResult, SearchQuery } from "~/src/types/SearchQuery";
import { FacetItem, SearchField } from "~/src/types/DataModel";
import { render } from "~/src/__tests__/testUtils/render";
import { resizeWindow } from "~/src/__tests__/testUtils/screen";
import { findFiltersForField } from "~/src/util/SearchQueryUtils";
import filterFields from "~/src/constants/filters";

const searchQuery: SearchQuery = {
  queries: [{ field: SearchField.Keyword, query: "Animal Crossing" }],
};
const emptySearchResults: ApiSearchResult = {
  status: 200,
  data: {
    totalWorks: 0,
    paging: {
      currentPage: 1,
      firstPage: 1,
      lastPage: 1,
      nextPage: 1,
      previousPage: 0,
      recordsPerPage: 10,
    },
    facets: { formats: [], languages: [] },
    works: [],
  },
};
const clickFiltersButton = async () =>
    userEvent.click(await screen.findByRole("button", { name: "Filter results" })
  );

describe("Renders Search Results Page", () => {
  beforeEach(() => {
    render(
      <KeywordSearch searchQuery={searchQuery} searchResults={searchResults} />
    );
    act(() => {
      resizeWindow(300, 1000);
    });
  });
  afterEach(() => cleanup());

  test("Main Content shows the current search query with 'alert' role", () => {
    expect(screen.getByRole("alert")).toHaveTextContent(
      /results for keyword: "Animal Crossing"/i
    );
  });
  test("Item Count shows correctly", () => {
    expect(screen.getByText("1 - 10 of 26 results", { exact: false })).toBeInTheDocument();
  });

  describe("Filters modal show and hide", () => {
    test("Filters button appears", () => {
      expect(
        screen.getByRole("button", { name: "Filter results" })
      ).toBeInTheDocument();
    });
    test("clicking 'filters' button shows filters contents", async () => {
      await clickFiltersButton();
      const modal = screen.getByTestId("filters-modal-content");
      expect(within(modal).getByRole("button", { name: /Sort by:/i })).toHaveTextContent(
        "Relevance"
      );
      const languages = within(modal).getByRole("group", {
        name: "Language",
      });
      expect(languages).toBeInTheDocument();
      const pubYear = within(modal).getByRole("region", {
        name: "Date filter",
      });
      expect(pubYear).toBeInTheDocument();
      expect(
        within(pubYear).getByRole("spinbutton", {
          name: "End",
        })
      ).toHaveValue(null);
      expect(
        within(pubYear).getByRole("spinbutton", {
          name: "Start",
        })
      ).toHaveValue(null);

      const backButton = within(modal).getByRole("button", { name: "Go Back" });
      expect(backButton).toBeInTheDocument();
    });
  });
  describe("Filters interactions in narrow view", () => {
    describe("Sorts filters", () => {
      test("Changing items sends new search ", async () => {
        await clickFiltersButton();
        const sortMenuButton = screen.getByRole("button", { name: /Sort by:/i });
        expect(sortMenuButton).toBeInTheDocument();
        await userEvent.click(sortMenuButton);
        const sortOption = screen.getByRole("menuitem", { name: "Title (A - Z)" });
        await userEvent.click(sortOption);
        expect(mockRouter).toMatchObject({
          pathname: "/keyword-search",
          query: {
            query: "keyword:Animal Crossing",
            sort: "title:ASC",
          },
        });
        await userEvent.click(screen.getByRole("button", { name: "Go Back" }));
        expect(
          screen.getByRole("button", { name: "Filter results" })
        ).toBeInTheDocument();
      }, 15000);
    });
    describe("Languages filter", () => {
      const availableLanguages: FacetItem[] =
        searchResults &&
        searchResults.data.facets &&
        searchResults.data.facets.languages;

      FilterLanguagesCommonTests(screen, availableLanguages, true);

      test("Clicking new language sends new search", async () => {
        await clickFiltersButton();
        const languages = screen.getByRole("group", {
          name: "Language",
        });

        const englishCheckbox = within(languages).getByRole("checkbox", {
          name: "English (6)",
        });

        await userEvent.click(englishCheckbox);
        expect(mockRouter).toMatchObject({
          pathname: "/keyword-search",
          query: {
            filter: "language:English",
            query: "keyword:Animal Crossing",
          },
        });

        await userEvent.click(screen.getByRole("button", { name: "Go Back" }));
        await userEvent.click(
          screen.getByRole("button", { name: "Filter results" })
        );

        const languages2 = screen.getByRole("group", {
          name: "Language",
        });

        const englishCheckbox2 = within(languages2).getByRole("checkbox", {
          name: "English (6)",
        });
        expect(englishCheckbox2).toBeChecked();
      }, 20000);
    });
    describe("Publication Year", () => {
      FilterYearsTests(
        true,
        findFiltersForField([], filterFields.startYear)[0],
        findFiltersForField([], filterFields.endYear)[0],
        mockRouter
      );
    });
    describe("Gov Doc Filter", () => {
      test("Clicking show only gov docs sends new search", async () => {
        await clickFiltersButton();
        const govDocCheckbox = screen.getByRole("checkbox", {
          name: "Limit to US government documents",
        });
        await userEvent.click(govDocCheckbox);
        expect(mockRouter).toMatchObject({
          pathname: "/keyword-search",
          query: {
            filter: "govDoc:onlyGovDoc",
            query: "keyword:Animal Crossing",
          },
        });
        await userEvent.click(screen.getByRole("button", { name: "Go Back" }));
        expect(
          screen.getByRole("button", { name: "Filter results" })
        ).toBeInTheDocument();
      }, 15000);
    });
  });
  describe("Clear Filters", () => {
    test("Renders when a filter is applied", async () => {
      expect(
        screen.queryByRole("button", { name: "Clear Filters" })
      ).not.toBeInTheDocument();

      await clickFiltersButton();
      const languages = screen.getByRole("group", {
        name: "Language",
      });
      const englishCheckbox = within(languages).getByRole("checkbox", {
        name: "English (6)",
      });
      await userEvent.click(englishCheckbox);
      await userEvent.click(screen.getByRole("button", { name: "Go Back" }));

      expect(
        screen.getByRole("button", { name: "Clear Filters" })
      ).toBeInTheDocument();
    }, 15000);

    test("Resets filters when clicked", async () => {
      await clickFiltersButton();
      const languages = screen.getByRole("group", {
        name: "Language",
      });
      const englishCheckbox = within(languages).getByRole("checkbox", {
        name: "English (6)",
      });
      await userEvent.click(englishCheckbox);
      await userEvent.click(screen.getByRole("button", { name: "Go Back" }));
      const clearFiltersButton = screen.getByRole("button", {
        name: "Clear Filters",
      });
      await userEvent.click(clearFiltersButton);
      expect(mockRouter).toMatchObject({
        pathname: "/keyword-search",
        query: {
          query: "keyword:Animal Crossing",
        },
      });
    }, 15000);
  });
  describe("Search Results", () => {
    describe("First result has full data", () => {
      test("Title links to work page", () => {
        expect(
          screen.getByText("Happy Home Companion: Cute Tables")
        ).toBeInTheDocument();
        expect(
          screen.getByText("Happy Home Companion: Cute Tables").closest("a")
            .href
        ).toContain("/work/test-uuid-1");
        expect(
          screen.getByText("Happy Home Companion: Cute Tables").closest("a")
            .href
        ).toContain("featured=1453292");
      });
      test("Author links to author search", () => {
        expect(screen.getByText("Nook, Timmy").closest("a").href).toContain(
          "http://localhost/search?query=author%3ANook%2C+Timmy"
        );
        expect(screen.getByText("Nook, Tammy").closest("a").href).toContain(
          "http://localhost/search?query=author%3ANook%2C+Tammy"
        );
      });
      test("Shows Full Publisher", () => {
        expect(
          screen.getByText("Published in Island Getaway by Nook Industries")
        ).toBeInTheDocument();
      });
      test("Shows download as link", () => {
        expect(
          screen.getAllByText("Download PDF")[0].closest("a").href
        ).toEqual("https://test-link-url-3/");
      });
      test("Shows 'read online' as link", () => {
        expect(
          screen.getAllByText("Read online")[0].closest("a").href
        ).toContain("read/3330416");
      });
    });
    describe("Second result has no data", () => {
      test("Shows Unknown Publisher", () => {
        expect(
          screen.getByText("Publisher and Location Unknown")
        ).toBeInTheDocument();
      });
      test("Not available ctas", () => {
        expect(screen.getByText("Not yet available")).toBeInTheDocument();
      });
    });
    describe("Third result has maximal data", () => {
      test("Title is truncated on full word and links to work page", () => {
        expect(
          screen.getByText("Happy Home Companion: super super super...")
        ).toBeInTheDocument();
        expect(
          screen
            .getByText("Happy Home Companion: super super super...")
            .closest("a").href
        ).toContain("/work/test-uuid-3");
        expect(
          screen
            .getByText("Happy Home Companion: super super super...")
            .closest("a").href
        ).toContain("featured=1453292");
      });
      test("All authors are shown and duplicate authors are not filtered", () => {
        expect(
          screen.getAllByText("Nook, Tom", { exact: false }).length
        ).toEqual(12);
      });

      test("Truncates publisher place and first full publisher name", () => {
        expect(
          screen.getByText(
            "Published in Taumatawhakatangihangakoauauotamateaturipukakapikimaungahoronukupokaiwhenuaki... by Nook Industries Nook Industries Nook Industries Nook Industries Nook... + 4 more"
          )
        ).toBeInTheDocument();
      });
      test("Does not show download link", () => {
        // The found `download` link is from the first result
        expect(screen.queryAllByText("Download PDF")[1]).not.toBeDefined();
      });
      test("Shows 'read online' as link", () => {
        expect(
          screen.getAllByText("Read online")[1].closest("a").href
        ).toContain("read/3234");
      });
    });
  });
  describe("Pagination appears", () => {
    test("Previous page link is disabled", () => {
      const previousLink = screen.queryByRole("link", {
        name: "Previous page",
      });
      expect(previousLink).toHaveAttribute("aria-disabled", "true");
    });
    test("Next page link appears and is clickable", async () => {
      const nextLink = screen.getByRole("link", { name: "Next page" });
      expect(nextLink).toBeInTheDocument();
      await userEvent.click(nextLink);
      expect(mockRouter).toMatchObject({
        pathname: "/keyword-search",
        query: {
          page: 2,
          query: "keyword:Animal Crossing",
        },
      });
    }, 15000);
    test("Middle numbers are clickable", async () => {
      const twoButton = screen.getByRole("link", { name: "Page 2" });
      expect(twoButton).toBeInTheDocument();
      await userEvent.click(twoButton);
      expect(mockRouter).toMatchObject({
        pathname: "/keyword-search",
        query: {
          page: 2,
          query: "keyword:Animal Crossing",
        },
      });
    }, 15000);
  });
});

describe("Renders correctly when perPage is greater than item count", () => {
  beforeEach(() => {
    render(
      <KeywordSearch
        searchQuery={searchQuery}
        searchResults={{
          data: {
            totalWorks: 2,
            facets: { formats: [], languages: [] },
            paging: {
              currentPage: 1,
              firstPage: 1,
              lastPage: 1,
              nextPage: 1,
              previousPage: 1,
              recordsPerPage: 10,
            },
            works: [],
          },
        }}
      />
    );
  });

  test("Item Count shows correctly", () => {
    expect(screen.getByText("1 - 2 of 2 results", { exact: false })).toBeInTheDocument();
  });
});

describe("Renders locale string correctly with large numbers", () => {
  beforeEach(() => {
    render(
      <KeywordSearch
        searchQuery={searchQuery}
        searchResults={{
          data: {
            totalWorks: 2013521,
            facets: { formats: [], languages: [] },
            paging: {
              currentPage: 3123,
              firstPage: 0,
              lastPage: 201352,
              nextPage: 3124,
              previousPage: 3122,
              recordsPerPage: 10,
            },
            works: [],
          },
        }}
      />
    );
  });
  test("Item Count shows correctly", () => {
    expect(
      screen.getByText("31,221 - 31,230 of 2,013,521 results", { exact: false })
    ).toBeInTheDocument();
  });
});

describe("Renders No Results when no results are shown", () => {
  beforeEach(() => {
    render(
      <KeywordSearch
        searchQuery={searchQuery}
        searchResults={emptySearchResults}
      />
    );
  });
  test("Item Count shows correctly", () => {
    expect(screen.getByText("Viewing 0 items")).toBeInTheDocument();
  });
  test("No Results message appears", () => {
    expect(
      screen.getByText(
        "No results were found."
      )
    ).toBeInTheDocument();
  });
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

describe("Renders search header correctly when viaf search is passed", () => {
  const viafSearchQuery: SearchQuery = {
    queries: [{ field: SearchField.Viaf, query: "12345" }],
    display: { field: SearchField.Author, query: "display author" },
  };
  beforeEach(() => {
    render(
      <KeywordSearch
        searchQuery={viafSearchQuery}
        searchResults={searchResults}
      />
    );
  });

  test("Main Content shows the viaf query", () => {
    expect(
      screen.getByText(/results for author: "display author"/i)
    ).toBeInTheDocument();
  });

  test("Search bar is prepopulated with the author name", () => {
    expect(
      screen.getByRole("combobox", { name: "Select a search category" })
    ).toHaveValue("author");
    expect(screen.getByRole("textbox", { name: "Item Search" })).toHaveValue(
      "display author"
    );
  });

  test("Author links to viaf search", () => {
    expect(screen.getByText("display author").closest("a").href).toContain(
      "http://localhost/search?query=viaf%3A12345&display=author%3Adisplay+author"
    );
  });
});

describe("Renders total works correctly when feature flag is set", () => {
  beforeEach(() => {
    act(() => {
      resizeWindow(300, 1000);
      Object.defineProperty(window, "sessionStorage", {
        value: {
          getItem: jest.fn(() => null),
          setItem: jest.fn(() => null),
        },
        writable: true,
      });
    });
  });

  test("Shown when feature flag query is true", () => {
    mockRouter.push("?feature_totalCount=true");
    render(
      <KeywordSearch searchQuery={searchQuery} searchResults={searchResults} />
    );
    expect(window.sessionStorage.getItem).toHaveBeenCalledTimes(1);
    expect(window.sessionStorage.setItem).toHaveBeenCalledTimes(1);
    expect(window.sessionStorage.setItem).toHaveBeenCalledWith(
      "featureFlags",
      JSON.stringify({ totalCount: true })
    );
    expect(screen.getByText("Total number of works: 26")).toBeInTheDocument();
  });

  test("Not shown when feature flag query is false", () => {
    mockRouter.push("?feature_totalCount=false");
    render(
      <KeywordSearch searchQuery={searchQuery} searchResults={searchResults} />
    );
    expect(window.sessionStorage.getItem).toHaveBeenCalledTimes(1);
    expect(window.sessionStorage.setItem).toHaveBeenCalledTimes(1);
    expect(window.sessionStorage.setItem).toHaveBeenCalledWith(
      "featureFlags",
      JSON.stringify({ totalCount: false })
    );
    expect(
      screen.queryByText("Total number of works: 26")
    ).not.toBeInTheDocument();
  });

  test("Not shown when feature flag query is not passed", () => {
    render(
      <KeywordSearch searchQuery={searchQuery} searchResults={searchResults} />
    );
    expect(window.sessionStorage.getItem).toHaveBeenCalledTimes(1);
    expect(window.sessionStorage.setItem).toHaveBeenCalledTimes(1);
    expect(
      screen.queryByText("Total number of works: 26")
    ).not.toBeInTheDocument();
  });
});

describe("Renders selected languages in language accordion when there are no matching results", () => {
  beforeEach(() => {
    const languageSearchQuery: SearchQuery = {
      queries: [
        {
          field: SearchField.Title,
          query: '"New York City"',
        },
      ],
      filters: [
        {
          field: "language",
          value: "Russian",
        },
      ],
    };
    render(
      <KeywordSearch
        searchQuery={languageSearchQuery}
        searchResults={emptySearchResults}
      />
    );
  });

  test("Show Russian (0) checkbox", async () => {
    const startInput = screen.getByRole("spinbutton", {
      name: "Start",
    });
    const applyButton = screen.getByRole("button", {
      name: "Apply",
    });
    await userEvent.type(startInput, "2000");
    await userEvent.click(applyButton);
    expect(mockRouter).toMatchObject({
      pathname: "/keyword-search",
      query: {
        filter: "language:Russian,startYear:2000",
        query: 'title:"New York City"',
      },
    });
    const languages = screen.getByRole("group", {
      name: "Language",
    });

    const russianCheckbox = within(languages).getByRole("checkbox", {
      name: "Russian (0)",
    });

    expect(russianCheckbox).toBeInTheDocument();
  });
});
