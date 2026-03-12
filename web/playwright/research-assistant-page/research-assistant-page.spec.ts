import { expect, Page, test } from "@playwright/test";
import { ResearchAssistantPage } from "./research-assistant-page";

test.describe("Research Assistant Page UI (initial state)", { tag: "@vra" }, () => {
  let page: Page;
  let researchAssistantPage: ResearchAssistantPage;

  test.beforeAll(async ({ browser }) => {
    page = await browser.newPage();
    researchAssistantPage = new ResearchAssistantPage(page);
    await researchAssistantPage.navigateTo();
  });

  test.afterAll(async () => {
    await page.close();
  });

  test("Research Assistant panel heading is visible", async () => {
    await expect(researchAssistantPage.researchAssistantPanelHeading).toBeVisible();
  });

  test("A message bubble from the agent is visible", async () => {
    await expect(researchAssistantPage.messageBubbles.nth(0)).toBeVisible();
  });

  test("Chat input text box is visible", async () => {
    await expect(researchAssistantPage.chatInputTextBox).toBeVisible();
  });

  test("Results banner is visible", async () => {
    await expect(researchAssistantPage.resultsBanner).toBeVisible();
  });

  test("Empty search prompt is visible", async () => {
    await expect(researchAssistantPage.emptySearchPrompt).toBeVisible();
  });

  test("'Start over' button is visible", async () => {
    await expect(researchAssistantPage.startOverBtn).toBeVisible();
  });

  test("'Hide chat' button is visible", async () => {
    await expect(researchAssistantPage.hideChatBtn).toBeVisible();
  });
});

test.describe("Research Assistant Page Functionality", { tag: "@vra" }, () => {
  let researchAssistantPage: ResearchAssistantPage;
  const testQuery = "I want to learn about Bryant Park.";

  test.beforeEach(async ({ page }) => {
    researchAssistantPage = new ResearchAssistantPage(page);
    await researchAssistantPage.navigateTo();
    await researchAssistantPage.logIn(process.env.VRA_USERNAME, process.env.VRA_PASSWORD);
    await researchAssistantPage.navigateTo(); // Return to the RA page after logging in (SCHOL-279)
  });

  test.describe("Chat interface (right panel)", () => {
    test("Chat input text box is ready for input", async () => {
      await researchAssistantPage.chatInputTextBox.fill(testQuery);
      const inputValue = await researchAssistantPage.chatInputTextBox.inputValue();
      expect(inputValue).toBe(testQuery);
    });

    test("An assistant response is displayed after submitting text", async () => {
      await researchAssistantPage.query(testQuery);

      const start = Date.now();
      await expect(researchAssistantPage.loadingIndicator).toBeHidden({ timeout: 120_000 });
      const end = Date.now();
      const elapsedSeconds = ((end - start) / 1000).toFixed(2);
      console.log(`Loading indicator disappeared after ${elapsedSeconds} seconds`);

      await expect(researchAssistantPage.messageBubbles.nth(1)).toBeVisible({ timeout: 120_000 });
    });
  });

  test.describe("Results (left panel)", () => {
    test.beforeEach(async () => {
      await researchAssistantPage.query(testQuery);
      await researchAssistantPage.loadingIndicator.isHidden({ timeout: 120_000 });
    });

    test("At least one result is displayed along with non-zero paging text", async () => {
      await expect.poll(() => researchAssistantPage.results.count()).toBeGreaterThan(0);
      await expect(researchAssistantPage.nonZeroResultsPagingText).toBeVisible();
    });

    test("First result card displays a status, title, author, publisher, and edition", async () => {
      await researchAssistantPage.firstResult.waitFor({ state: "visible" });

      await expect(researchAssistantPage.firstResultStatusBadge).toBeVisible();
      await expect(researchAssistantPage.firstResultStatusBadge).toHaveText(/\S+/);

      await expect(researchAssistantPage.firstResultTitle).toBeVisible();
      await expect(researchAssistantPage.firstResultTitle).toHaveText(/\S+/);

      await expect(researchAssistantPage.firstResultAuthor).toBeVisible();
      await expect(researchAssistantPage.firstResultAuthor).toHaveText(/\S+/);

      await expect(researchAssistantPage.firstResultPublisher).toBeVisible();
      await expect(researchAssistantPage.firstResultPublisher).toHaveText(/\S+/);

      await expect(researchAssistantPage.firstResultEdition).toBeVisible();
      await expect(researchAssistantPage.firstResultEdition).toHaveText(/\S+/);
    });

    test("Clicking result title opens item page for the expected edition", async ({ page }) => {
      await researchAssistantPage.firstResult.waitFor({ state: "visible" });
      const firstEditionId = await researchAssistantPage.getFirstResultEditionId();
      await researchAssistantPage.firstResultTitle.getByRole("link").click();
      await expect(page).toHaveURL(new RegExp(`\\?featured=${firstEditionId}$`));
    });

    test("Result Preview button opens item page for the expected edition", async ({ page }) => {
      await researchAssistantPage.firstResult.waitFor({ state: "visible" });
      const firstEditionId = await researchAssistantPage.getFirstResultEditionId();
      await researchAssistantPage.firstResultPreviewBtn.click();
      await expect(page).toHaveURL(new RegExp(`\\?featured=${firstEditionId}$`));
    });
  });
});
