import { Page, test } from "@playwright/test";
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
    await researchAssistantPage.messageBubble.isVisible();
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

  test.beforeEach(async ({ page }) => {
    researchAssistantPage = new ResearchAssistantPage(page);
    await researchAssistantPage.navigateTo();
  });

  test("Search text box is ready for input", async () => {
    const testQuery = "Bryant park";
    await researchAssistantPage.chatInputTextBox.fill(testQuery);
    const inputValue = await researchAssistantPage.chatInputTextBox.inputValue();
    expect(inputValue).toBe(testQuery);
  });
});
