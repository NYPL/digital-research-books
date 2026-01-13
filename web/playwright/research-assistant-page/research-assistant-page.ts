import { Locator, Page } from "@playwright/test";

class ResearchAssistantPage {
  readonly page: Page;
  readonly researchAssistantPanelHeading: Locator;
  readonly messageBubble: Locator;
  readonly chatInputTextBox: Locator;
  readonly resultsBanner: Locator;
  readonly emptySearchPrompt: Locator;
  readonly startOverBtn: Locator;
  readonly hideChatBtn: Locator;

  constructor(page: Page) {
    this.page = page;
    this.researchAssistantPanelHeading = page.getByRole("heading", {
      name: "Virtual Research Assistant",
    });
    this.messageBubble = page.getByText("VRA:");
    this.chatInputTextBox = page.getByRole("textbox", { name: "Type your response here" });
    this.resultsBanner = page.getByText("public domain scholarly e-books from our collections");
    this.emptySearchPrompt = page.getByRole("heading", { name: "Start searching to see results" });
    this.startOverBtn = page.getByRole("button", { name: "Start over" });
    this.hideChatBtn = page.getByRole('button', { name: 'Hide chat' });
  }

  async navigateTo() { await this.page.goto("/research-assistant"); }
}

export { ResearchAssistantPage };
