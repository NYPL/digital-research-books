import { test } from "@playwright/test";
import { LandingPage } from "./landing-page";

test.describe("Author Search", () => {
  test("Search for an author and the first search result is by the author", async ({
    page,
  }) => {
    const landingPage = new LandingPage(page);
    await landingPage.navigateToHome();
    await landingPage.selectCategory("author");
    await landingPage.fillSearchBox("Corelli, Marie");
    await landingPage.clickSearchButton();
    await landingPage.verifyFirstSearchResultAuthorVisible();
  });
});

test.describe("Footer Links", () => {
  test(`Navigate to the landing page and verify the footer links are displayed`, async ({
    page,
  }) => {
    const landingPage = new LandingPage(page);
    await landingPage.navigateToHome();
    await landingPage.verifyFooterLinksVisible();
  });
});

test.describe("Header Links", () => {
  test(`Navigate to the landing page and verify the header links are displayed`, async ({
    page,
  }) => {
    const landingPage = new LandingPage(page);
    await landingPage.navigateToHome();
    await landingPage.verifyHeaderLinkVisible(landingPage.headerLogo);
    await landingPage.verifyHeaderLinkVisible(landingPage.myAccountHeaderLink);
    await landingPage.verifyHeaderLinkVisible(landingPage.locationsHeaderLink);
    await landingPage.verifyHeaderLinkVisible(
      landingPage.libraryCardHeaderLink
    );
    await landingPage.verifyHeaderLinkVisible(
      landingPage.emailUpdatesHeaderLink
    );
    await landingPage.verifyHeaderLinkVisible(landingPage.donateHeaderLink);
    await landingPage.verifyHeaderLinkVisible(landingPage.shopHeaderLink);
    await landingPage.verifyHeaderLinkVisible(
      landingPage.booksMusicMoviesHeaderLink
    );
    await landingPage.verifyHeaderLinkVisible(landingPage.researchHeaderLink);
    await landingPage.verifyHeaderLinkVisible(landingPage.educationHeaderLink);
    await landingPage.verifyHeaderLinkVisible(landingPage.eventsHeaderLink);
    await landingPage.verifyHeaderLinkVisible(landingPage.giveHeaderLink);
    await landingPage.verifyHeaderLinkVisible(landingPage.getHelpHeaderLink);
    await landingPage.verifyHeaderLinkVisible(landingPage.searchHeaderLink);
  });

  test("Navigate to the Digital Research Books home page and verify the account and search header sub-links and elements are displayed", async ({
    page,
  }) => {
    const landingPage = new LandingPage(page);
    await landingPage.navigateToHome();
    await landingPage.clickHeaderLink(landingPage.myAccountHeaderLink);
    await landingPage.verifyHeaderLinkVisible(landingPage.catalogHeaderLink);
  });
});

test.describe("Home Page Elements", () => {
  test("Navigate to the Digital Research Books home page and all home page elements are displayed", async ({
    page,
  }) => {
    const landingPage = new LandingPage(page);
    await landingPage.navigateToHome();
    await landingPage.verifyElementVisible(landingPage.siteNameHeading);
    await landingPage.verifyElementVisible(landingPage.homeBreadcrumbLink);
    await landingPage.verifyElementVisible(landingPage.researchBreadcrumbLink);
    await landingPage.verifyElementVisible(
      landingPage.digitalResearchBooksBetaBreadcrumbLink
    );
    await landingPage.verifyElementVisible(landingPage.introText);
    await landingPage.verifyElementVisible(landingPage.searchHeading);
    await landingPage.verifyElementVisible(landingPage.searchCategoryDropdown);
    await landingPage.verifyElementVisible(landingPage.homepageSearchBox);
    await landingPage.verifyElementVisible(landingPage.searchButton);
    await landingPage.verifyElementVisible(landingPage.advancedSearchLink);
    await landingPage.verifyElementVisible(landingPage.collectionsHeading);
    await landingPage.verifyElementVisible(landingPage.firstCollectionCardLink);
    await landingPage.verifyElementVisible(landingPage.footer);
  });

  test("Verify the help and feedback button is displayed on the Digital Research Books home page", async ({
    page,
  }) => {
    const landingPage = new LandingPage(page);
    await landingPage.navigateToHome();
    await landingPage.verifyElementVisible(landingPage.helpAndFeedbackButton);
  });
});
