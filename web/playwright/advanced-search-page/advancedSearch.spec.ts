import { test } from "@playwright/test";
import { AdvancedSearchPage } from "./advanced-search-page";

test.describe("Advanced Search", () => {
  test("Navigate to the advanced search page and all page elements are displayed", async ({
    page,
  }) => {
    const advancedSearchPage = new AdvancedSearchPage(page);
    await advancedSearchPage.navigateToAdvancedSearchPage();
    await advancedSearchPage.clickAdvancedSearchLink();
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.advancedSearchHeading
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.keywordSearchLabel
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.keywordSearchBox
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.authorSearchLabel
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.authorSearchBox
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.subjectSearchLabel
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.subjectSearchBox
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.titleSearchLabel
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.titleSearchBox
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.dateFilterFromLabel
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.dateFilterFromField
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.dateFilterToLabel
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.dateFilterToField
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.advancedSearchButton
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.advancedSearchClearButton
    );
  });

  test("Search for a keyword and author and the search results contain both the keyword and author", async ({
    page,
  }) => {
    const advancedSearchPage = new AdvancedSearchPage(page);
    await advancedSearchPage.navigateToAdvancedSearchPage();
    await advancedSearchPage.clickAdvancedSearchLink();
    // await advancedSearchPage.fillKeywordSearchBox("IBM 1401");
    // await advancedSearchPage.fillAuthorSearchBox("Laurie, Edward J.");
    await advancedSearchPage.fillKeywordSearchBox(
      "The Mediterranean region under climate change"
    );
    await advancedSearchPage.fillAuthorSearchBox("Moatti, Jean-Paul. ()");
    await advancedSearchPage.clickAdvancedSearchButton();
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.keywordHeading
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.authorHeading
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.searchResultLink
    );
  });

  test("Select a language checkbox in advanced search and verify the search results", async ({
    page,
  }) => {
    const advancedSearchPage = new AdvancedSearchPage(page);
    await advancedSearchPage.navigateToAdvancedSearchPage();
    await advancedSearchPage.clickAdvancedSearchLink();
    // await advancedSearchPage.fillKeywordSearchBox("Jane Austen");
    // await advancedSearchPage.clickRussianLanguageCheckbox();
    await advancedSearchPage.fillKeywordSearchBox("Finite mathematics");
    await advancedSearchPage.clickSpanishLanguageCheckbox();
    await advancedSearchPage.clickAdvancedSearchButton();
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.spanishLanguageSubheader
    );
    await advancedSearchPage.verifyElementChecked(
      advancedSearchPage.spanishLanguageCheckbox
    );
    await advancedSearchPage.verifyElementVisible(
      advancedSearchPage.firstReadOnlineButton
    );
  });
});
