import { expect, Locator, Page } from "@playwright/test";

class ItemDetailPage {
  readonly page: Page;
  readonly searchCategoryDropdown: Locator;
  readonly homepageSearchBox: Locator;
  readonly searchButton: Locator;
  readonly advancedSearchLink: Locator;
  readonly itemTitle: Locator;
  readonly itemAuthor: Locator;
  readonly itemFeaturedEditionHeading: Locator;
  readonly itemFeaturedEditionCover: Locator;
  readonly itemFeaturedEditionYear: Locator;
  readonly itemFeaturedEditionPublisher: Locator;
  readonly itemFeaturedEditionLanguage: Locator;
  readonly itemFeaturedEditionLicense: Locator;
  readonly itemDetailsHeading: Locator;
  readonly itemDetailsAuthorsHeading: Locator;
  readonly itemDetailsAuthors: Locator;
  readonly itemDetailsSubjectsHeading: Locator;
  readonly itemDetailsSubjects: Locator;
  readonly itemDetailsLanguagesHeading: Locator;
  readonly itemDetailsLanguages: Locator;
  readonly itemAllEditionsHeading: Locator;
  readonly itemsCurrentlyAvailableOnlineToggleText: Locator;
  readonly itemsCurrentlyAvailableOnlineToggle: Locator;
  readonly secondItemEdition: Locator;
  readonly firstReadOnlineButtonForAllEditions: Locator;
  readonly backToSearchResultsButton: Locator;
  readonly lettersOfJaneAustenLink: Locator;
  readonly lettersOfJaneAustenHeading: Locator;
  readonly finiteMathematicsLink: Locator;
  readonly finiteMathematicsHeading: Locator;

  constructor(page: Page) {
    this.page = page;
    this.searchCategoryDropdown = page.locator(
      "[aria-label='Select a search category']"
    );
    this.homepageSearchBox = page.locator("[aria-label='Item Search']");
    this.searchButton = page.locator("#search-bar-submit-button");
    this.advancedSearchLink = page.locator("[href='/advanced-search']");
    this.itemTitle = page.locator("#work-title");
    this.itemAuthor = page.locator(
      "div:text('By') > a[href*='display=author'] >> nth=0"
    );
    this.itemFeaturedEditionHeading = page.locator(
      "div:text('Featured Edition')"
    );
    this.itemFeaturedEditionCover = page.locator(
      "[alt='Placeholder Cover'] >> nth=0"
    );
    this.itemFeaturedEditionYear = page.locator(
      "a:has-text('Edition') >> nth=0"
    );
    this.itemFeaturedEditionPublisher = page.locator(
      "div:text('Published by') >> nth=0"
    );
    this.itemFeaturedEditionLanguage = page.locator(
      "div:text('Languages') >> nth=0"
    );
    this.itemFeaturedEditionLicense = page.locator(
      "[href='/copyright'] >> nth=0"
    );
    this.itemDetailsHeading = page.locator("#details-list-heading");
    this.itemDetailsAuthorsHeading = page.locator("dt:text('Authors')");
    this.itemDetailsAuthors = page.locator(
      "dd > a[href*='display=author'] >> nth=0"
    );
    this.itemDetailsSubjectsHeading = page.locator("dt:text('Subjects')");
    this.itemDetailsSubjects = page.locator(
      "li > a[href*='/search?query=subject'] >> nth=0"
    );
    this.itemDetailsLanguagesHeading = page.locator("dt:text('Languages')");
    this.itemDetailsLanguages = page.locator("li:text('English')");
    this.itemAllEditionsHeading = page.locator("#all-editions");
    this.itemsCurrentlyAvailableOnlineToggleText = page.locator(
      "span + span:text('Show only items currently available online')"
    );
    this.itemsCurrentlyAvailableOnlineToggle = page.locator(
      "span:text('Show only items currently available online')"
    );
    this.secondItemEdition = page.locator("a:text('Edition') >> nth=1");
    this.firstReadOnlineButtonForAllEditions = page.locator(
      "a:text('Read Online') >> nth=1"
    );
    this.backToSearchResultsButton = page.locator(
      "a:text('Back to search results')"
    );
    this.lettersOfJaneAustenLink = page.locator(
      "//a[text()='Letters of Jane Austen'] >> nth=0"
    );
    this.lettersOfJaneAustenHeading = page.locator(
      "//h1[text()='Letters of Jane Austen']"
    );
    this.finiteMathematicsLink = page.locator(
      "//a[text()='Finite Mathematics'] >> nth=0"
    );
    this.finiteMathematicsHeading = page.locator(
      "//h1[text()='Finite Mathematics']"
    );
  }

  async navigateToItemDetailPage() {
    await this.page.goto(
      "/work/afed4441-43a7-4020-87d6-770ca88faac6?featured=4801342"
    );
  }

  async fillSearchBox(query: string) {
    await this.homepageSearchBox.fill(query);
  }

  async clickSearchButton() {
    await this.searchButton.click();
  }

  async clickElement(element: Locator) {
    await element.click();
  }

  async verifyElementVisible(element: Locator) {
    await expect(element).toBeVisible();
  }
}

export { ItemDetailPage };
