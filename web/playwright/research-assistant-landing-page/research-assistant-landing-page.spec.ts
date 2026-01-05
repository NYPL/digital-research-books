import { test, expect, Page } from "@playwright/test";
import { ResearchAssistantLandingPage } from "./research-assistant-landing-page";

test.describe("Research Assistant Landing Page UI", () => {
  let researchAssistantLandingPage: ResearchAssistantLandingPage;
  let page: Page;

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
    await expect(researchAssistantLandingPage.suggestBtnRomanEmpire).toBeVisible();
    await expect(researchAssistantLandingPage.suggestBtnFeminism).toBeVisible();
    await expect(researchAssistantLandingPage.suggestBtnMethodistChurch).toBeVisible();
    await expect(researchAssistantLandingPage.suggestBtnAmericanCivilWar).toBeVisible();
  });

  test("'Learn more' button is visible", async () => {
    await expect(researchAssistantLandingPage.learnMoreBtn).toBeVisible();
  });

  test("Features section heading is visible", async () => {
    await expect(researchAssistantLandingPage.featuresSectionHeading).toBeVisible();
  });

  test("Access section heading is visible", async () => {
    await expect(researchAssistantLandingPage.accessSectionHeading).toBeVisible();
  });

  test("Focused research section heading is visible", async () => {
    await expect(researchAssistantLandingPage.focusedResearchSectionHeading).toBeVisible();
  });

  test("FAQ section heading is visible", async () => {
    await expect(researchAssistantLandingPage.faqSectionHeading).toBeVisible();
  });

  test("Help section heading is visible", async () => {
    await expect(researchAssistantLandingPage.helpSectionHeading).toBeVisible();
  });
});

test("Search executes and routes to Research Assistant page", async ({ page }) => {
  const researchAssistantLandingPage = new ResearchAssistantLandingPage(page);
  await researchAssistantLandingPage.navigateTo();
  await researchAssistantLandingPage.search("Mark Twain");
  await expect(page).toHaveURL(/.*research-assistant/);
});
