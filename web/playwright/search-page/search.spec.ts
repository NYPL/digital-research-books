import { test } from "@playwright/test";
import { SearchPage } from "./search-page";

test.describe("EDD Request Process", () => {
  test("Begin the EDD request process  and the delivery locations page is displayed", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    // await searchPage.fillSearchBox("Africa");
    await searchPage.fillSearchBox("New York State Inebriate Asylum");
    await searchPage.clickSearchButton();
    await searchPage.clickRequestableCheckbox();
    await searchPage.clickFirstLoginForOptionsButton();
    await searchPage.fillUsernameField(process.env.CATALOG_USERNAME);
    await searchPage.fillPasswordField(process.env.CATALOG_USER_PIN);
    await searchPage.clickLoginButton();
    await searchPage.verifyFirstRequestButtonVisible();
  });
});

test.describe("Government Documents Filter", () => {
  test("Search for government documents and the first search result displays the United States as the author", async ({
    page,
  }) => {
    const govDocsPage = new SearchPage(page);
    await govDocsPage.navigateToSearchPage();
    await govDocsPage.fillSearchBox("swimming");
    await govDocsPage.clickSearchButton();
    await govDocsPage.clickGovernmentDocumentsCheckbox();
    await govDocsPage.verifyFirstGovernmentDocumentAuthorVisible();
  });
});

test.describe("Keyword Search", () => {
  test("Search for a keyword and the first search result contains the keyword", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    // await searchPage.fillSearchBox("IBM 1401");
    await searchPage.selectCategory("keyword");
    await searchPage.fillSearchBox(
      "The Mediterranean region under climate change"
    );
    await searchPage.clickSearchButton();
    await searchPage.verifyFirstSearchResultKeywordVisible();
  });
});

test.describe("Language Filter", () => {
  test("Filter search results by a French language and the first search result contains that language", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    await searchPage.fillSearchBox("Computer Science");
    await searchPage.clickSearchButton();
    await searchPage.clickLanguageCheckbox(searchPage.frenchLanguageCheckbox);
    await searchPage.verifyFirstSearchResultLanguageVisible(
      searchPage.firstSearchResultFrenchLanguage
    );
  });

  test("Filter search results by a Spanish language and the first search result contains that language", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    await searchPage.fillSearchBox("Computer Science");
    await searchPage.clickSearchButton();
    await searchPage.clickLanguageCheckbox(searchPage.spanishLanguageCheckbox);
    await searchPage.verifyFirstSearchResultLanguageVisible(
      searchPage.firstSearchResultSpanishLanguage
    );
  });

  test("Filter search results by a German language and the first search result contains that language", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    await searchPage.fillSearchBox("Computer Science");
    await searchPage.clickSearchButton();
    await searchPage.clickLanguageCheckbox(searchPage.germanLanguageCheckbox);
    await searchPage.verifyFirstSearchResultLanguageVisible(
      searchPage.firstSearchResultGermanLanguage
    );
  });

  test("Filter search results by a Polish language and the first search result contains that language", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    await searchPage.fillSearchBox("Poland");
    await searchPage.clickSearchButton();
    await searchPage.clickLanguageCheckbox(searchPage.polishLanguageCheckbox);
    await searchPage.verifyFirstSearchResultLanguageVisible(
      searchPage.firstSearchResultPolishLanguage
    );
  });

  test("Filter search results by a Portuguese language and the first search result contains that language", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    await searchPage.fillSearchBox("Lisbon");
    await searchPage.clickSearchButton();
    await searchPage.clickLanguageCheckbox(
      searchPage.portugueseLanguageCheckbox
    );
    await searchPage.verifyFirstSearchResultLanguageVisible(
      searchPage.firstSearchResultPortugueseLanguage
    );
  });
});

test.describe("Publication Year Filter", () => {
  test("Filter by publication year and the first search result displays an edition with that publication year", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    await searchPage.fillSearchBox("New York");
    await searchPage.clickSearchButton();
    await searchPage.fillPublicationYearFrom("1900");
    await searchPage.fillPublicationYearTo("1900");
    await searchPage.clickPublicationYearApplyButton();
    await searchPage.verifyFirstSearchResultEditionVisible();
  });
});

test.describe("Read Online Link Targets", () => {
  test("Click the Read Online button on the search results page and the link navigates to HathiTrust", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);

    // Verify on search results page
    await searchPage.navigateToSearchPage();
    await searchPage.clickFirstReadOnlineButton();
    await searchPage.verifyHathiTrustWebsiteVisible();
  });
});

test.describe("Subject Search", () => {
  test("Search for a subject and the details of the first search result contains the subject", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    await searchPage.selectCategory("subject");
    await searchPage.fillSearchBox("petroleum");
    await searchPage.clickSearchButton();
    await searchPage.clickFirstSearchResultLink();
    await searchPage.verifyFirstSearchResultSubjectVisible();
  });
});

test.describe("Title Search", () => {
  test("Search for a title and the first search result contains the title", async ({
    page,
  }) => {
    const searchPage = new SearchPage(page);
    await searchPage.navigateToSearchPage();
    await searchPage.selectCategory("title");
    // await searchPage.fillSearchBox("IBM 1401");
    await searchPage.fillSearchBox(
      "The Mediterranean region under climate change"
    );
    await searchPage.clickSearchButton();
    await searchPage.verifyFirstSearchResultTitleVisible();
  });
});
