import { expect, Locator, Page } from "@playwright/test";

class LandingPage {
  readonly page: Page;
  readonly categoryDropdown: Locator;
  readonly homepageSearchBox: Locator;
  readonly searchButton: Locator;
  readonly firstSearchResultAuthor: Locator;

  readonly accessibilityFooterLink: Locator;
  readonly pressFooterLink: Locator;
  readonly careersFooterLink: Locator;
  readonly spaceRentalFooterLink: Locator;
  readonly privacyPolicyFooterLink: Locator;
  readonly otherPoliciesFooterLink: Locator;
  readonly termsAndConditionsFooterLink: Locator;
  readonly governanceFooterLink: Locator;
  readonly rulesAndRegulationsFooterLink: Locator;
  readonly aboutNyplFooterLink: Locator;
  readonly languageFooterLink: Locator;

  readonly headerLogo: Locator;
  readonly myAccountHeaderLink: Locator;
  readonly locationsHeaderLink: Locator;
  readonly libraryCardHeaderLink: Locator;
  readonly emailUpdatesHeaderLink: Locator;
  readonly donateHeaderLink: Locator;
  readonly shopHeaderLink: Locator;
  readonly booksMusicMoviesHeaderLink: Locator;
  readonly researchHeaderLink: Locator;
  readonly educationHeaderLink: Locator;
  readonly eventsHeaderLink: Locator;
  readonly connectHeaderLink: Locator;
  readonly giveHeaderLink: Locator;
  readonly getHelpHeaderLink: Locator;
  readonly searchHeaderLink: Locator;
  readonly catalogHeaderLink: Locator;

  readonly siteNameHeading: Locator;
  readonly homeBreadcrumbLink: Locator;
  readonly researchBreadcrumbLink: Locator;
  readonly digitalResearchBooksBetaBreadcrumbLink: Locator;
  readonly introText: Locator;
  readonly searchHeading: Locator;
  readonly searchCategoryDropdown: Locator;
  readonly advancedSearchLink: Locator;
  readonly collectionsHeading: Locator;
  readonly firstCollectionCardLink: Locator;
  readonly footer: Locator;
  readonly helpAndFeedbackButton: Locator;

  constructor(page: Page) {
    this.page = page;
    this.categoryDropdown = page.locator(
      "[aria-label='Select a search category']"
    );
    this.homepageSearchBox = page.locator("[aria-label='Item Search']");
    this.searchButton = page.locator("#searchbar-button-search-bar");
    this.firstSearchResultAuthor = page.locator(
      "a:text('Corelli, Marie') >> nth=0"
    );

    this.accessibilityFooterLink = page.locator(
      "//a[@href='//www.nypl.org/accessibility']"
    );

    this.pressFooterLink = page.locator("//a[@href='//www.nypl.org/press']");
    this.careersFooterLink = page.locator(
      "//a[@href='//www.nypl.org/careers']"
    );
    this.spaceRentalFooterLink = page.locator(
      "//a[@href='//www.nypl.org/spacerental']"
    );
    this.privacyPolicyFooterLink = page.locator(
      "//a[@href='//www.nypl.org/help/about-nypl/legal-notices/privacy-policy']"
    );
    this.otherPoliciesFooterLink = page.locator(
      "//a[@href='//www.nypl.org/policies']"
    );
    this.termsAndConditionsFooterLink = page.locator(
      "//a[@href='//www.nypl.org/terms-conditions']"
    );
    this.governanceFooterLink = page.locator(
      "//a[@href='//www.nypl.org/help/about-nypl/leadership/board-trustees']"
    );
    this.rulesAndRegulationsFooterLink = page.locator(
      "//a[@href='//www.nypl.org/help/about-nypl/legal-notices/rules-and-regulations']"
    );
    this.aboutNyplFooterLink = page.locator(
      "//a[@href='//www.nypl.org/help/about-nypl']"
    );
    this.languageFooterLink = page.locator(
      "//a[@href='//www.nypl.org/language']"
    );

    this.headerLogo = page.locator(
      "a[aria-label='The New York Public Library']"
    );
    this.myAccountHeaderLink = page.locator("[aria-label='My Account']");
    this.locationsHeaderLink = page.locator(
      "[href='//www.nypl.org/locations']:text('Locations')"
    );
    this.libraryCardHeaderLink = page.locator(
      "[href='//www.nypl.org/library-card/new']"
    );
    this.emailUpdatesHeaderLink = page.locator(
      "[href='https://pub.email.nypl.org/subscriptioncenter']"
    );
    this.donateHeaderLink = page.locator(
      "[href='//www.nypl.org/donate-button']"
    );
    this.shopHeaderLink = page.locator(
      "[href='https://shop.nypl.org/?utm_campaign=NYPLHeaderButton&utm_source=nypl.org&utm_medium=referral']"
    );
    this.booksMusicMoviesHeaderLink = page.locator(
      "[href='//www.nypl.org/books-music-movies']"
    );
    this.researchHeaderLink = page.locator(
      "[href='//www.nypl.org/research'] >> nth=0"
    );
    this.educationHeaderLink = page.locator(
      "[href='//www.nypl.org/education'] >> nth=0"
    );
    this.eventsHeaderLink = page.locator("[href='//www.nypl.org/events']");
    this.connectHeaderLink = page.locator("[href='//www.nypl.org/connect']");
    this.giveHeaderLink = page.locator("[href='//www.nypl.org/give']");
    this.getHelpHeaderLink = page.locator("[href='//www.nypl.org/get-help']");
    this.searchHeaderLink = page.locator(
      "[href='//www.nypl.org/research'] >> nth=0"
    );
    this.catalogHeaderLink = page.locator("span:text('Go To The Catalog')");

    this.siteNameHeading = page.locator("h1:text('Digital Research Books')");
    this.homeBreadcrumbLink = page.locator(
      "a[href='https://www.nypl.org'] > .breadcrumb-label"
    );
    this.researchBreadcrumbLink = page.locator(
      "a[href='https://www.nypl.org/research'] > .breadcrumb-label"
    );
    this.digitalResearchBooksBetaBreadcrumbLink = page.locator(
      "span.breadcrumb-label:text('Digital Research Books Beta')"
    );
    this.introText = page.locator(
      "span:text('Find millions of digital books for research from multiple sources')"
    );
    this.searchHeading = page.locator("h1:text('Search the World')");
    this.searchCategoryDropdown = page.locator(
      "[aria-label='Select a search category']"
    );
    this.homepageSearchBox = page.locator("[aria-label='Item Search']");
    this.searchButton = page.locator("#searchbar-button-search-bar");
    this.advancedSearchLink = page.locator("[href='/advanced-search']");
    this.collectionsHeading = page.locator(
      "h2:text('Recently Added Collections')"
    );
    this.firstCollectionCardLink = page.locator(
      "a[href^='/collection/'] >> nth=0"
    );
    this.footer = page.locator("#nypl-footer");
    this.helpAndFeedbackButton = page.locator(
      "button:text('Help and Feedback')"
    );
  }

  async navigateToHome() {
    await this.page.goto("/");
  }

  async selectCategory(category: string) {
    await this.categoryDropdown.selectOption(category);
  }

  async fillSearchBox(query: string) {
    await this.homepageSearchBox.fill(query);
  }

  async clickSearchButton() {
    await this.searchButton.click();
  }

  async verifyFirstSearchResultAuthorVisible() {
    await expect(this.firstSearchResultAuthor).toBeVisible();
  }

  async verifyFooterLinksVisible() {
    await expect(this.accessibilityFooterLink).toBeVisible();
    await expect(this.pressFooterLink).toBeVisible();
    await expect(this.careersFooterLink).toBeVisible();
    await expect(this.spaceRentalFooterLink).toBeVisible();
    await expect(this.privacyPolicyFooterLink).toBeVisible();
    await expect(this.otherPoliciesFooterLink).toBeVisible();
    await expect(this.termsAndConditionsFooterLink).toBeVisible();
    await expect(this.governanceFooterLink).toBeVisible();
    await expect(this.rulesAndRegulationsFooterLink).toBeVisible();
    await expect(this.aboutNyplFooterLink).toBeVisible();
    await expect(this.languageFooterLink).toBeVisible();
  }

  async clickHeaderLink(headerLink: Locator) {
    await headerLink.click();
  }

  async verifyHeaderLinkVisible(headerLink: Locator) {
    await expect(headerLink).toBeVisible();
  }

  async verifyElementVisible(element: Locator) {
    await expect(element).toBeVisible();
  }
}

export { LandingPage };
