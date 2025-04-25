import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { inputTerms } from "~/src/constants/labels";
import { SearchQuery } from "~/src/types/SearchQuery";

export const searchFormRenderTests = (query?: SearchQuery) => {
  test("Searchbar select defaults", () => {
    const expectedSearchField =
      query && query.queries ? query.queries[0].field : inputTerms[0].value;
    expect(
      screen.getByRole("combobox", { name: "Select a search category" })
    ).toHaveValue(expectedSearchField);
  });
  test("Searchbar has the correct options", () => {
    const options = screen.getAllByRole("option");
    expect(options[0]).toHaveValue(inputTerms[0].value);
    expect(options[1]).toHaveValue(inputTerms[1].value);
    expect(options[2]).toHaveValue(inputTerms[2].value);
    expect(options[3]).toHaveValue(inputTerms[3].value);
  });
  test("Searchbar has correct input", () => {
    const expectedSearchValue =
      query && query.queries ? query.queries[0].query : "";
    expect(screen.getByRole("textbox", { name: "Item Search" })).toHaveValue(
      expectedSearchValue
    );
  });
  test("Shows link to Advanced Search", () => {
    expect(screen.getByText("Advanced Search").closest("a").href).toContain(
      "/advanced-search"
    );
  });
};

export const searchFormTests = (mockRouter) => {
  describe("Search using Landing Page Searchbar", () => {
    test("Searching with a field and value calls Search api", async () => {
      const categoryInput = screen.getByRole("combobox", {
        name: "Select a search category",
      });
      const textInput = screen.getByRole("textbox", { name: "Item Search" });
      await userEvent.selectOptions(categoryInput, "author");
      await userEvent.clear(textInput);
      await userEvent.type(textInput, "Tom Nook");

      const searchButton = screen.getByRole("button", { name: "Search" });
      await userEvent.click(searchButton);

      expect(
        screen.getByRole("combobox", { name: "Select a search category" })
      ).toHaveValue("author");
      expect(screen.getByRole("textbox", { name: "Item Search" })).toHaveValue(
        "Tom Nook"
      );
      expect(
        screen.queryByText("Error: Please enter a search term")
      ).not.toBeInTheDocument();

      expect(mockRouter).toMatchObject({
        pathname: "/search",
        query: {
          query: "author:Tom Nook",
        },
      });
    });

    test("Searching with no search term shows error", async () => {
      const textInput = screen.getByRole("textbox", { name: "Item Search" });
      await userEvent.clear(textInput);

      expect(textInput).toHaveValue("");
      const searchButton = screen.getByRole("button", { name: "Search" });
      await userEvent.click(searchButton);

      expect(mockRouter).toMatchObject({});
      expect(
        screen.getByText("There was a problem. Error: Please enter a search term")
      ).toBeInTheDocument();
    });
  });
};
