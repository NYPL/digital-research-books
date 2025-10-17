import { render, screen } from "@testing-library/react";
import { inputTerms } from "~/src/constants/labels";
import { SearchQuery } from "~/src/types/SearchQuery";
import KeywordSearchForm from "./KeywordSearchForm";

describe("Keyword Search Form", (query?: SearchQuery) => {
    beforeEach(() => {
        render(<KeywordSearchForm />);
    })
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
});
