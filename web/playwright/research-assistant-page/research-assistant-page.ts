import { Page } from "@playwright/test";

class ResearchAssistantPage {
  readonly page: Page;

  constructor(page: Page) {
    this.page = page;
  }

  async navigateTo() { await this.page.goto("/research-assistant"); }
}

export { ResearchAssistantPage };
