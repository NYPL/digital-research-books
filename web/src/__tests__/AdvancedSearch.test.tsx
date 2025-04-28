import React from "react";
import AdvancedSearch from "../components/AdvancedSearch/AdvancedSearch";
import { screen } from "@testing-library/react";
import { FilterLanguagesCommonTests } from "./componentHelpers/FilterLanguages";
import { FilterYearsTests } from "./componentHelpers/FilterYears";
import { FilterFormatTests } from "./componentHelpers/FilterFormats";
import userEvent from "@testing-library/user-event";
import { errorMessagesText, inputTerms } from "../constants/labels";
import { ApiLanguageResponse } from "../types/LanguagesQuery";
import mockRouter from "next-router-mock";
import { render } from "./testUtils/render";

const defaultLanguages: ApiLanguageResponse = {
  status: 200,
  data: [
    { language: "english", count: 25 },
    { language: "french", count: 30 },
  ],
};

describe("renders advanced search correctly", () => {
  describe("Renders correctly in when passed well-formed query", () => {
    beforeEach(() => {
      render(<AdvancedSearch languages={defaultLanguages} />);
    });

    describe("Search Fields render", () => {
      test("Search fields are empty", () => {
        expect(screen.getByLabelText("Keyword")).toHaveValue("");
        expect(screen.getByLabelText("Author")).toHaveValue("");
        expect(screen.getByLabelText("Title")).toHaveValue("");
        expect(screen.getByLabelText("Subject")).toHaveValue("");
      });
    });
    describe("Language filter is shown", () => {
      FilterLanguagesCommonTests(screen, defaultLanguages.data, false);
    });
    describe("Year filter is shown", () => {
      FilterYearsTests(false);
    });
    test("Format filter is shown", () => {
      FilterFormatTests();
    });
    test("Gov doc filter is shown", () => {
      const govDocCheckbox = screen.getByRole("checkbox", {
        name: "Show only US government documents",
      });
      expect(govDocCheckbox).toBeInTheDocument();
      expect(govDocCheckbox).not.toBeChecked();
    });
    test("Submit and clear buttons are shown", () => {
      expect(
        screen.getByRole("button", { name: "Search" })
      ).toBeInTheDocument();
      expect(screen.getByRole("button", { name: "Clear" })).toBeInTheDocument();
    });
  });

  test("Hides languages when no languages are passed", () => {
    render(<AdvancedSearch languages={{ status: 200, data: [] }} />);
    expect(
      screen.queryByRole("group", { name: "Languages" })
    ).not.toBeInTheDocument();
  });
});

describe("Advanced Search submit", () => {
  test("Submits well formed query", async () => {
    render(<AdvancedSearch languages={defaultLanguages} />);

    await userEvent.click(
      screen.getByRole("checkbox", {
        name: "english",
      })
    );
    await userEvent.type(screen.getByLabelText("Keyword"), "cat");
    await userEvent.type(screen.getByLabelText("Author"), "Nook");
    await userEvent.type(screen.getByLabelText("Title"), "Handbook");
    await userEvent.type(screen.getByLabelText("Subject"), "poetry");
    await userEvent.type(
      screen.getByRole("spinbutton", { name: "From" }),
      "1990"
    );
    await userEvent.type(
      screen.getByRole("spinbutton", { name: "To" }),
      "1999"
    );
    await userEvent.click(
      screen.getByRole("checkbox", {
        name: "Readable",
      })
    );
    await userEvent.click(screen.getByRole("button", { name: "Search" }));

    const expectedQuery = {
      filter: "language:english,startYear:1990,endYear:1999,format:readable",
      query: "keyword:cat,author:Nook,title:Handbook,subject:poetry",
    };
    expect(mockRouter).toMatchObject({
      pathname: "/search",
      query: expectedQuery,
    });
  });

  test("Submits only year start and subject", async () => {
    render(<AdvancedSearch languages={defaultLanguages} />);

    await userEvent.type(
      screen.getByRole("spinbutton", { name: "From" }),
      "1990"
    );
    await userEvent.type(screen.getByLabelText("Keyword"), "cat");

    await userEvent.click(screen.getByRole("button", { name: "Search" }));

    const expectedQuery = {
      filter: "startYear:1990",
      query: "keyword:cat",
    };
    expect(mockRouter).toMatchObject({
      pathname: "/search",
      query: expectedQuery,
    });
  });

  test("Submits only year end and author", async () => {
    render(<AdvancedSearch languages={defaultLanguages} />);

    await userEvent.type(
      screen.getByRole("spinbutton", { name: "To" }),
      "1990"
    );
    await userEvent.type(screen.getByLabelText("Author"), "Shakespeare");

    await userEvent.click(screen.getByRole("button", { name: "Search" }));

    const expectedQuery = {
      filter: "endYear:1990",
      query: "author:Shakespeare",
    };
    expect(mockRouter).toMatchObject({
      pathname: "/search",
      query: expectedQuery,
    });
  });

  test("shows error on empty query", async () => {
    render(<AdvancedSearch languages={defaultLanguages} />);
    await userEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(mockRouter).toMatchObject({});
    expect(screen.getByText(errorMessagesText.emptySearch)).toBeInTheDocument();
  });

  test("show error on invalid year", async () => {
    render(<AdvancedSearch languages={defaultLanguages} />);

    await userEvent.type(
      screen.getByRole("spinbutton", { name: "From" }),
      "1990"
    );
    await userEvent.type(
      screen.getByRole("spinbutton", { name: "To" }),
      "1880"
    );
    await userEvent.type(screen.getByLabelText("Keyword"), "cat");

    await userEvent.click(screen.getByRole("button", { name: "Search" }));

    expect(mockRouter).toMatchObject({});
    expect(screen.getByText(errorMessagesText.invalidDate)).toBeInTheDocument();
  });
});

describe("Advanced Search clear", () => {
  test("clears all searches", async () => {
    render(<AdvancedSearch languages={defaultLanguages} />);

    const inputValues = {
      Keyword: "cat",
      Author: "Nook",
      Subject: "poetry",
      Title: "Handbook",
    };

    const englishCheckbox = screen.getByRole("checkbox", { name: "english" });
    const readableCheckbox = screen.getByRole("checkbox", { name: "Readable" });
    const govDocCheckbox = screen.getByRole("checkbox", { name: "Show only US government documents" });

    await userEvent.click(englishCheckbox);
    await userEvent.type(
      screen.getByLabelText("Keyword"),
      inputValues["Keyword"]
    );
    await userEvent.type(
      screen.getByLabelText("Author"),
      inputValues["Author"]
    );
    await userEvent.type(
      screen.getByLabelText("Subject"),
      inputValues["Subject"]
    );
    await userEvent.type(screen.getByLabelText("Title"), inputValues["Title"]);
    await userEvent.clear(
      await screen.findByRole("spinbutton", { name: "From" })
    );
    await userEvent.type(
      await screen.findByRole("spinbutton", { name: "From" }),
      "1990"
    );
    await userEvent.type(
      screen.getByRole("spinbutton", { name: "To" }),
      "1999"
    );
    await userEvent.click(readableCheckbox);
    await userEvent.click(govDocCheckbox);

    expect(englishCheckbox).toBeChecked();
    expect(screen.getByLabelText("From")).toHaveValue(1990);
    expect(screen.getByLabelText("To")).toHaveValue(1999);
    expect(readableCheckbox).toBeChecked();
    expect(govDocCheckbox).toBeChecked();

    expect(await screen.findByRole("textbox", { name: "Keyword" })).toHaveValue(
      inputValues["Keyword"]
    );
    expect(await screen.findByRole("textbox", { name: "Author" })).toHaveValue(
      inputValues["Author"]
    );
    expect(await screen.findByRole("textbox", { name: "Subject" })).toHaveValue(
      inputValues["Subject"]
    );
    expect(await screen.findByRole("textbox", { name: "Title" })).toHaveValue(
      inputValues["Title"]
    );

    await userEvent.click(screen.getByRole("button", { name: "Clear" }));

    expect(englishCheckbox).not.toBeChecked();
    expect(readableCheckbox).not.toBeChecked();
    expect(govDocCheckbox).not.toBeChecked();
    expect(screen.getByLabelText("From")).toHaveValue(null);
    expect(screen.getByLabelText("To")).toHaveValue(null);

    inputTerms.forEach((val) => {
      expect(screen.getByLabelText(val.text)).toHaveValue("");
    });

    await userEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(mockRouter).toMatchObject({});
    expect(screen.getByText(errorMessagesText.emptySearch)).toBeInTheDocument();
  });

  test("Deleting search clears it from state", async () => {
    render(<AdvancedSearch languages={defaultLanguages} />);

    await userEvent.type(screen.getByLabelText("Keyword"), "cat");

    const keywordInput: HTMLInputElement = screen.getByLabelText(
      "Keyword"
    ) as HTMLInputElement;
    expect(keywordInput).toHaveValue("cat");
    await userEvent.clear(keywordInput);
    expect(screen.getByLabelText("Keyword")).toHaveValue("");

    await userEvent.click(screen.getByRole("button", { name: "Search" }));
    expect(mockRouter).toMatchObject({});
    expect(screen.getByText(errorMessagesText.emptySearch)).toBeInTheDocument();
  });
});
