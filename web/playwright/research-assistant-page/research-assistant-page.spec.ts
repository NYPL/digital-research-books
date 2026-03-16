import { BrowserContext, expect, Page, test } from "@playwright/test";
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
  test.describe.configure({ mode: "serial" });
  test.setTimeout(120_000); // Override global timeout in case API is slow to respond

  let context: BrowserContext;
  let page: Page;
  let researchAssistantPage: ResearchAssistantPage;
  const testQuery = "I want to learn about Bryant Park.";

  test.beforeAll(async ({ browser }) => {
    context = await browser.newContext();
    page = await context.newPage();
    researchAssistantPage = new ResearchAssistantPage(page);
    await researchAssistantPage.navigateTo();
    await researchAssistantPage.logIn(process.env.VRA_USERNAME, process.env.VRA_PASSWORD);
    await researchAssistantPage.navigateTo(); // Return to the RA page after logging in (SCHOL-279)
  });

  test.afterAll(async () => {
    console.log(`Total queries executed: ${ResearchAssistantPage.getQueryExecutionCount()}`);
    await context.close();
  });

  test.describe("Chat interface (right panel)", () => {
    test("Chat input text box is ready for input", async () => {
      await researchAssistantPage.chatInputTextBox.fill(testQuery);
      const inputValue = await researchAssistantPage.chatInputTextBox.inputValue();
      expect(inputValue).toBe(testQuery);
    });

    test("An assistant response is displayed after submitting text", async () => {
      await researchAssistantPage.query(testQuery);

      await test.step("Wait for loading indicator to disappear", async () => {
        await expect(researchAssistantPage.loadingIndicator).toBeHidden({ timeout: 90_000 });
      });

      await expect(researchAssistantPage.messageBubbles.nth(1)).toBeVisible({ timeout: 10_000 }); // Assistant response should shortly follow loading indicator disappearing
    });
  });

  test.describe("Results (left panel)", () => {
    test.beforeAll(async () => {
      // Execute test query if not already done in previous tests or isolated run
      if (ResearchAssistantPage.getQueryExecutionCount() === 0) {
        await researchAssistantPage.query(testQuery);
        await researchAssistantPage.loadingIndicator.waitFor({ state: "hidden", timeout: 90_000 });
      }
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

    test("Result title and Preview button direct to item page for the expected edition", async () => {
      await researchAssistantPage.firstResult.waitFor({ state: "visible" });

      const firstEditionId = await researchAssistantPage.getFirstResultEditionId();
      const uuidPattern = new RegExp(
        `[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`
      );
      const itemPageUrlPattern = new RegExp(
        `(?:^|/)item/${uuidPattern.source}\\?featured=${firstEditionId}$`
      );

      await expect(researchAssistantPage.firstResultTitleLink).toHaveAttribute("href", itemPageUrlPattern);
      await expect(researchAssistantPage.firstResultTitleLink).toBeVisible();
      await expect(researchAssistantPage.firstResultTitleLink).toBeEnabled();
      await expect(async () => {
        await researchAssistantPage.firstResultTitleLink.click({ trial: true });
      }).toPass();

      await expect(researchAssistantPage.firstResultPreviewBtn).toHaveAttribute("href", itemPageUrlPattern);
      await expect(researchAssistantPage.firstResultPreviewBtn).toBeVisible();
      await expect(researchAssistantPage.firstResultPreviewBtn).toBeEnabled();
      await expect(async () => {
        await researchAssistantPage.firstResultPreviewBtn.click({ trial: true });
      }).toPass();
    });
  });
});
