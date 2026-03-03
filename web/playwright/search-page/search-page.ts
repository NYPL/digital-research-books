import { expect, Locator, Page } from "@playwright/test";

class SearchPage {
  readonly page: Page;
  readonly homepageSearchBox: Locator;
  readonly searchButton: Locator;
  readonly searchSelector: Locator;

  readonly requestableCheckbox: Locator;
  readonly firstLoginForOptionsButton: Locator;
  readonly usernameField: Locator;
  readonly passwordField: Locator;
  readonly loginButton: Locator;
  readonly firstRequestButton: Locator;

  readonly governmentDocumentsCheckbox: Locator;
  readonly firstGovernmentDocumentAuthor: Locator;

  readonly frenchLanguageCheckbox: Locator;
  readonly spanishLanguageCheckbox: Locator;
  readonly germanLanguageCheckbox: Locator;
  readonly polishLanguageCheckbox: Locator;
  readonly portugueseLanguageCheckbox: Locator;
  readonly firstSearchResultFrenchLanguage: Locator;
  readonly firstSearchResultSpanishLanguage: Locator;
  readonly firstSearchResultGermanLanguage: Locator;
  readonly firstSearchResultPolishLanguage: Locator;
  readonly firstSearchResultPortugueseLanguage: Locator;

  readonly publicationYearFromFilter: Locator;
  readonly publicationYearToFilter: Locator;
  readonly publicationYearApplyButton: Locator;
  readonly firstSearchResultEdition: Locator;

  readonly firstReadOnlineButton: Locator;
  readonly hathiTrustWebsite: Locator;

  readonly categoryDropdown: Locator;
  readonly firstSearchResultLink: Locator;
  readonly firstSearchResultKeyword: Locator;
  readonly firstSearchResultSubject: Locator;
  readonly firstSearchResultTitle: Locator;

  constructor(page: Page) {
    this.page = page;
    this.homepageSearchBox = page.locator("[aria-label='Item Search']");
    this.searchButton = page.locator("#search-bar-submit-button");
    this.searchSelector = page.getByRole("combobox", {
      name: "Select a search category",
    });

    this.requestableCheckbox = page.locator("span:text('Requestable')");
    this.firstLoginForOptionsButton = page.locator(
      "a:text('Log in to request scan') >> nth=0"
    );
    this.usernameField = page.locator("#code");
    this.passwordField = page.locator("#pin");
    this.loginButton = page.locator("[value='Submit']");
    this.firstRequestButton = page.locator("a:text('Request scan') >> nth=0");

    this.governmentDocumentsCheckbox = page.locator(
      "span:text('Show only US government documents')"
    );
    this.firstGovernmentDocumentAuthor = page.locator(
      "a:text('United States') >> nth=0"
    );
    // this.firstSearchResultKeyword = page.locator("a:text('IBM 1401') >> nth=0");
    this.firstSearchResultKeyword = page.locator(
      "a:text('The Mediterranean region under climate change') >> nth=0"
    );
    this.firstReadOnlineButton = page.locator("a:text('Read Online') >> nth=0");
    // this.hathiTrustWebsite = page.locator(
    //   "iframe[src='https://babel.hathitrust.org/cgi/pt?id=hvd.32044079201976']"
    // );
    this.hathiTrustWebsite = page.locator(
      "iframe[src='https://babel.hathitrust.org/cgi/pt?id=uc2.ark:/13960/t9k35tr7n']"
    );

    this.frenchLanguageCheckbox = page.locator("span:text('French')");
    this.spanishLanguageCheckbox = page.locator("span:text('Spanish')");
    this.germanLanguageCheckbox = page.locator("span:text('German')");
    this.polishLanguageCheckbox = page.locator("span:text('Polish')");
    this.portugueseLanguageCheckbox = page.locator("span:text('Portuguese')");
    this.firstSearchResultFrenchLanguage = page.locator(
      "div:text('French') >> nth=0"
    );
    this.firstSearchResultSpanishLanguage = page.locator(
      "div:text('Spanish') >> nth=0"
    );
    this.firstSearchResultGermanLanguage = page.locator(
      "div:text('German') >> nth=0"
    );
    this.firstSearchResultPolishLanguage = page.locator(
      "div:text('Polish') >> nth=0"
    );
    this.firstSearchResultPortugueseLanguage = page.locator(
      "div:text('Portuguese') >> nth=0"
    );

    this.publicationYearFromFilter = page.locator("#date-filter-from");
    this.publicationYearToFilter = page.locator("#date-filter-to");
    this.publicationYearApplyButton = page.locator("#year-filter-button");
    this.firstSearchResultEdition = page.locator(
      "a:text('1900 Edition') >> nth=0"
    );

    this.categoryDropdown = page.locator(
      "[aria-label='Select a search category']"
    );
    this.firstSearchResultLink = page.locator("h2 a >> nth=0");
    this.firstSearchResultSubject = page.locator(
      "a:text('Petroleum') >> nth=0"
    );
    // this.firstSearchResultTitle = page.locator("a:text('IBM 1401') >> nth=0");
    this.firstSearchResultTitle = page.locator(
      "a:text('The Mediterranean region under climate change') >> nth=0"
    );
  }

  async navigateToSearchPage() {
    await this.page.goto("/search?query=subject%3Awashington+dc");
  }

  async selectSearchCategory(category: string) {
    await this.searchSelector.selectOption(category);
  }

  async fillSearchBox(query: string) {
    await this.homepageSearchBox.fill(query);
  }

  async clickSearchButton() {
    await this.searchButton.click();
  }

  async clickRequestableCheckbox() {
    await this.requestableCheckbox.click();
  }

  async clickFirstLoginForOptionsButton() {
    await this.firstLoginForOptionsButton.click();
  }

  async fillUsernameField(username: string) {
    await this.usernameField.fill(username);
  }

  async fillPasswordField(password: string) {
    await this.passwordField.fill(password);
  }

  async clickLoginButton() {
    await this.loginButton.click();
  }

  async verifyFirstRequestButtonVisible() {
    await expect(this.firstRequestButton).toBeVisible();
    await expect(this.firstRequestButton).toHaveAttribute(
      "href",
      /.*www\.nypl\.org\/research\/collections\/shared-collection-catalog\/hold\/request\/.*/
    );
  }

  async verifyFirstLoginForOptionsButtonVisible() {
    await expect(this.firstLoginForOptionsButton).toBeVisible();
  }

  async clickGovernmentDocumentsCheckbox() {
    await this.governmentDocumentsCheckbox.click();
  }

  async verifyFirstGovernmentDocumentAuthorVisible() {
    await expect(this.firstGovernmentDocumentAuthor).toBeVisible();
  }

  async verifyFirstSearchResultKeywordVisible() {
    await expect(this.firstSearchResultKeyword).toBeVisible();
  }

  async clickLanguageCheckbox(languageCheckbox: Locator) {
    await languageCheckbox.click();
  }

  async verifyFirstSearchResultLanguageVisible(languageResult: Locator) {
    await expect(languageResult).toBeVisible();
  }

  async fillPublicationYearFrom(year: string) {
    await this.publicationYearFromFilter.fill(year);
  }

  async fillPublicationYearTo(year: string) {
    await this.publicationYearToFilter.fill(year);
  }

  async clickPublicationYearApplyButton() {
    await this.publicationYearApplyButton.click();
  }

  async verifyFirstSearchResultEditionVisible() {
    await expect(this.firstSearchResultEdition).toBeVisible();
  }

  async clickFirstReadOnlineButton() {
    await this.firstReadOnlineButton.click();
  }

  async verifyHathiTrustWebsiteVisible() {
    await expect(this.hathiTrustWebsite).toBeVisible();
  }

  async selectCategory(category: string) {
    await this.categoryDropdown.selectOption(category);
  }

  async clickFirstSearchResultLink() {
    await this.firstSearchResultLink.click();
  }

  async verifyFirstSearchResultSubjectVisible() {
    await expect(this.firstSearchResultSubject).toBeVisible();
  }

  async verifyFirstSearchResultTitleVisible() {
    await expect(this.firstSearchResultTitle).toBeVisible();
  }
}

export { SearchPage };
