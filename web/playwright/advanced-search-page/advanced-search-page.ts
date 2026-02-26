import { expect, Locator, Page } from "@playwright/test";

class AdvancedSearchPage {
  readonly page: Page;
  readonly advancedSearchLink: Locator;
  readonly advancedSearchHeading: Locator;
  readonly keywordSearchLabel: Locator;
  readonly keywordSearchBox: Locator;
  readonly authorSearchLabel: Locator;
  readonly authorSearchBox: Locator;
  readonly subjectSearchLabel: Locator;
  readonly subjectSearchBox: Locator;
  readonly titleSearchLabel: Locator;
  readonly titleSearchBox: Locator;
  readonly dateFilterFromLabel: Locator;
  readonly dateFilterFromField: Locator;
  readonly dateFilterToLabel: Locator;
  readonly dateFilterToField: Locator;
  readonly advancedSearchButton: Locator;
  readonly advancedSearchClearButton: Locator;
  readonly keywordHeading: Locator;
  readonly authorHeading: Locator;
  readonly searchResultLink: Locator;
  readonly russianLanguageCheckbox: Locator;
  readonly russianLanguageSubheader: Locator;
  readonly spanishLanguageCheckbox: Locator;
  readonly spanishLanguageSubheader: Locator;
  readonly firstReadOnlineButton: Locator;

  constructor(page: Page) {
    this.page = page;
    this.advancedSearchLink = page.locator("[href='/advanced-search']");
    this.advancedSearchHeading = page.locator("h1:text('Advanced Search')");
    this.keywordSearchLabel = page.locator("#search-Keyword-label");
    this.keywordSearchBox = page.locator("#search-Keyword");
    this.authorSearchLabel = page.locator("#search-Author-label");
    this.authorSearchBox = page.locator("#search-Author");
    this.subjectSearchLabel = page.locator("#search-Subject-label");
    this.subjectSearchBox = page.locator("#search-Subject");
    this.titleSearchLabel = page.locator("#search-Title-label");
    this.titleSearchBox = page.locator("#search-Title");
    this.dateFilterFromLabel = page.locator("#date-filter-from-label");
    this.dateFilterFromField = page.locator("#date-filter-from");
    this.dateFilterToLabel = page.locator("#date-filter-to-label");
    this.dateFilterToField = page.locator("#date-filter-to");
    this.advancedSearchButton = page.locator("#submit-button");
    this.advancedSearchClearButton = page.locator("#reset-button");
    // this.keywordHeading = page.locator("h1:text('IBM 1401')");
    // this.authorHeading = page.locator("h1:text('Laurie, Edward J.')");
    this.keywordHeading = page.locator(
      "h1:text('The Mediterranean region under climate change')"
    );
    this.authorHeading = page.locator("h1:text('Moatti, Jean-Paul. ()')");
    // this.searchResultLink = page.locator(
    //   "a:text('Computers and how they work')"
    // );
    this.searchResultLink = page.locator(
      "a:text('The Mediterranean region under climate change')"
    );
    this.russianLanguageCheckbox = page.locator("span:text('Russian')");
    this.russianLanguageSubheader = page.locator("span:text('Russian')");
    this.spanishLanguageCheckbox = page.locator("span:text('Spanish')");
    this.spanishLanguageSubheader = page.locator("span:text('Spanish')");
    this.firstReadOnlineButton = page.locator("a:text('Read Online') >> nth=0");
  }

  async navigateToAdvancedSearchPage() {
    await this.page.goto("/advanced-search");
  }

  async clickAdvancedSearchLink() {
    await this.advancedSearchLink.click();
  }

  async fillKeywordSearchBox(keyword: string) {
    await this.keywordSearchBox.fill(keyword);
  }

  async fillAuthorSearchBox(author: string) {
    await this.authorSearchBox.fill(author);
  }

  async clickAdvancedSearchButton() {
    await this.advancedSearchButton.click();
  }

  async clickRussianLanguageCheckbox() {
    await this.russianLanguageCheckbox.click();
  }

  async clickSpanishLanguageCheckbox() {
    await this.spanishLanguageCheckbox.click();
  }

  async verifyElementVisible(selector: Locator) {
    await expect(selector).toBeVisible();
  }

  async verifyElementChecked(selector: Locator) {
    await expect(selector).toBeChecked();
  }
}

export { AdvancedSearchPage };
