import { test } from "@playwright/test";
import { ResearchAssistantPage } from "./research-assistant-page";

test("Research Assistant page loads", async ({ page }) => {
  const researchAssistantPage = new ResearchAssistantPage(page);
  await researchAssistantPage.navigateTo(), { waitUntil: 'domcontentloaded' };
});
