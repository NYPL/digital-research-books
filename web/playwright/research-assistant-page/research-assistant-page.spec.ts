import { expect, Page, test } from "@playwright/test";
import { ResearchAssistantPage } from "./research-assistant-page";

test("Navigating to Research Assistant page loads DOM", async ({ page }) => {
  const researchAssistantPage = new ResearchAssistantPage(page);
  await researchAssistantPage.navigateTo(), { waitUntil: 'domcontentloaded' };
});

test.describe("Research Assistant Page UI", () => {
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
    await researchAssistantPage.researchAssistantPanelHeading.isVisible();
  });

  test("A message bubble from the agent is visible", async () => {
    await researchAssistantPage.messageBubbles.nth(0).isVisible();
  });

  test("Chat input text box is visible", async () => {
    await researchAssistantPage.chatInputTextBox.isVisible();
  });

  test("Results banner is visible", async () => {
    await researchAssistantPage.resultsBanner.isVisible();
  });

  test("Empty search prompt is visible", async () => {
    await researchAssistantPage.emptySearchPrompt.isVisible();
  });

  test("'Start over' button is visible", async () => {
    await researchAssistantPage.startOverBtn.isVisible();
  });

  test("'Hide chat' button is visible", async () => {
    await researchAssistantPage.hideChatBtn.isVisible();
  });
});

test.describe("Research Assistant Page Functionality", () => {
  let researchAssistantPage: ResearchAssistantPage;
  let testQuery = "Bryant park";

  test.beforeEach(async ({ page }) => {
    researchAssistantPage = new ResearchAssistantPage(page);
    await researchAssistantPage.navigateTo();
  });

  test("Chat input text box is ready for input", async () => {
    await researchAssistantPage.chatInputTextBox.fill(testQuery);
    const inputValue = await researchAssistantPage.chatInputTextBox.inputValue();
    expect(inputValue).toBe(testQuery);
  });

  test("An assistant response is displayed after submitting text", async () => {
    // Must log in to submit queries
    const username = process.env.VRA_USERNAME as string;
    const password = process.env.VRA_PASSWORD as string;
    await researchAssistantPage.logIn(username, password);
    await researchAssistantPage.navigateTo(); // Navigate back to RA page after log in (SCHOL-279)

    // Submit the test query
    await researchAssistantPage.query(testQuery);

    // Wait for the loading indicator to disappear and report the time it took
    const start = Date.now();
    await expect(researchAssistantPage.loadingIndicator).toBeHidden({ timeout: 60000 });
    const end = Date.now();
    const elapsedSeconds = ((end - start) / 1000).toFixed(2);
    console.log(`Loading indicator disappeared after ${elapsedSeconds} seconds`);

    // Verify a new message is displayed
    await expect(researchAssistantPage.messageBubbles.nth(1)).toBeVisible();
  });
});
