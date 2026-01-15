import { Locator, Page } from "@playwright/test";

class ResearchAssistantPage {
  readonly page: Page;
  readonly researchAssistantPanelHeading: Locator;
  readonly messageBubbles: Locator;
  readonly chatInputTextBox: Locator;
  readonly submitQueryBtn: Locator;
  readonly resultsBanner: Locator;
  readonly emptySearchPrompt: Locator;
  readonly startOverBtn: Locator;
  readonly hideChatBtn: Locator;
  readonly logInBtn: Locator;

  constructor(page: Page) {
    this.page = page;
    this.researchAssistantPanelHeading = page.getByRole("heading", {
      name: "Virtual Research Assistant",
    });
    this.messageBubbles = page.getByText("VRA:");
    this.chatInputTextBox = page.getByRole("textbox", { name: "Type your response here" });
    this.submitQueryBtn = page.getByLabel("Send");
    this.resultsBanner = page.getByText("public domain scholarly e-books from our collections");
    this.emptySearchPrompt = page.getByRole("heading", { name: "Start searching to see results" });
    this.startOverBtn = page.getByRole("button", { name: "Start over" });
    this.hideChatBtn = page.getByRole("button", { name: "Hide chat" });
    this.logInBtn = page.getByRole("link", { name: "Login" }) // update name for SCHOL-280
  }

  async navigateTo() { await this.page.goto("/research-assistant"); }

  async query(query: string) {
    await this.chatInputTextBox.fill(query);
    await new Promise(resolve =>
      setTimeout(resolve, 500)
    ); // sleep for 0.5s to simulate user pause between typing and submitting
    await this.submitQueryBtn.click();
  }

  // TODO: Relocate log in method to a base page class since it exists across pages
  async logIn(username: string, password: string) {
    if (!username || !password) {
      throw new Error("Username and password must be defined");
    }
    // Start waiting for a new page event before clicking (the log in page may open in a new tab)
    const pagePromise = this.page.context().waitForEvent("page");
    await this.logInBtn.click();

    let loginPage: Page;
    try {
      // Wait for a short time for a new page to open
      loginPage = await Promise.race([
        pagePromise,
        new Promise<Page>((_, reject) => setTimeout(() => reject("no new page"), 1000))
      ]);
    } catch {
      // If no new page, use the current page
      loginPage = this.page;
    }

    await loginPage.waitForLoadState();
    await loginPage.getByLabel("Username").fill(username);
    await loginPage.getByLabel("Password").fill(password);
    await loginPage.getByRole("button", { name: "Login" }).click(); // update name for SCHOL-280
    await loginPage.waitForLoadState("networkidle"); // Wait for navigation to complete
  }
}

export { ResearchAssistantPage };
