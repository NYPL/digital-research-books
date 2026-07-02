import { expect, Page, test } from "@playwright/test";
import { ResearchAssistantLandingPage } from "./research-assistant-landing-page";

test.describe(
  "Research Assistant Landing Page UI",
  { tag: "@enhanced-search" },
  () => {
    let page: Page;
    let researchAssistantLandingPage: ResearchAssistantLandingPage;

    test.beforeAll(async ({ browser }) => {
      page = await browser.newPage();
      researchAssistantLandingPage = new ResearchAssistantLandingPage(page);
      await researchAssistantLandingPage.navigateTo();
    });

    test.afterAll(async () => {
      await page.close();
    });

    test("Hero heading is visible", async () => {
      await expect(researchAssistantLandingPage.heroHeading).toBeVisible();
    });

    test("Hero subheading is visible", async () => {
      await expect(researchAssistantLandingPage.heroSubheading).toBeVisible();
    });

    test("Subnav is visible", async () => {
      await expect(researchAssistantLandingPage.subNav).toBeVisible();
    });

    test("Search text box is visible", async () => {
      await expect(researchAssistantLandingPage.searchTextBox).toBeVisible();
    });

    test("Search submit button is visible", async () => {
      await expect(researchAssistantLandingPage.searchSubmitBtn).toBeVisible();
    });

    test("Search suggestions are visible", async () => {
      await expect(
        researchAssistantLandingPage.suggestBtnAncientRome
      ).toBeVisible();
      await expect(
        researchAssistantLandingPage.suggestButtonMedievalWarfare
      ).toBeVisible();
      await expect(
        researchAssistantLandingPage.suggestBtnOrnithology
      ).toBeVisible();
      await expect(
        researchAssistantLandingPage.suggestBtnShipbuilding
      ).toBeVisible();
    });

    test("'Learn more' buttons are visible", async () => {
      await expect(researchAssistantLandingPage.learnMoreBtns).toHaveCount(3);
      for (const btn of await researchAssistantLandingPage.learnMoreBtns.all()) {
        await expect(btn).toBeVisible();
      }
    });

    test("Features section heading is visible", async () => {
      await expect(
        researchAssistantLandingPage.featuresSectionHeading
      ).toBeVisible();
    });

    test("Access section heading is visible", async () => {
      await expect(
        researchAssistantLandingPage.accessSectionHeading
      ).toBeVisible();
    });

    test("FAQ section heading is visible", async () => {
      await expect(
        researchAssistantLandingPage.faqSectionHeading
      ).toBeVisible();
    });

    test("Help section heading is visible", async () => {
      await expect(
        researchAssistantLandingPage.helpSectionHeading
      ).toBeVisible();
    });

    test.describe("Breadcrumb links", { tag: "@enhanced-search" }, () => {
      test("'Home' breadcrumb links to nypl.org", async () => {
        await expect(
          researchAssistantLandingPage.homeBreadcrumbLink
        ).toHaveAttribute("href", "https://www.nypl.org");
      });

      test("'Research' breadcrumb links to nypl.org/research", async () => {
        await expect(
          researchAssistantLandingPage.researchBreadcrumbLink
        ).toHaveAttribute("href", "https://www.nypl.org/research");
      });

      test("'Digitized Research Books' breadcrumb links to /", async () => {
        await expect(
          researchAssistantLandingPage.digitizedResearchBooksBreadcrumbLink
        ).toHaveAttribute("href", "/");
      });

      test("'Enhanced Search (beta)' breadcrumb links to /research-assistant", async () => {
        await expect(
          researchAssistantLandingPage.enhancedSearchBreadcrumbLink
        ).toHaveAttribute("href", "/research-assistant");
      });
    });
  }
);

test.describe(
  "Research Assistant Landing Page Functionality",
  { tag: "@enhanced-search" },
  () => {
    let researchAssistantLandingPage: ResearchAssistantLandingPage;

    test.beforeEach(async ({ page }) => {
      researchAssistantLandingPage = new ResearchAssistantLandingPage(page);
      await researchAssistantLandingPage.navigateTo();
    });

    test("Search text box is ready for input", async () => {
      const testQuery = "renaissance";
      await researchAssistantLandingPage.searchTextBox.fill(testQuery);
      const inputValue = await researchAssistantLandingPage.searchTextBox.inputValue();
      expect(inputValue).toBe(testQuery);
    });

    test("Executing search routes to Research Assistant page", async ({
      page,
    }) => {
      await researchAssistantLandingPage.search("renaissance");
      await expect(page).toHaveURL(/.+\/research-assistant$/);
    });
  }
);
