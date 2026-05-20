import { Locator, Page } from "@playwright/test";
import {
  RESULT_AUTHOR_TEST_ID,
  RESULT_EDITION_TEST_ID,
  RESULT_PUBLISHER_TEST_ID,
  RESULT_TEST_ID,
  RESULT_TITLE_TEST_ID,
} from "~/src/constants/testIds";

class ResearchAssistantPage {
  readonly page: Page;

  // Authentication
  readonly logInBtn: Locator;

  // Chat interface (right panel)
  readonly researchAssistantPanelHeading: Locator;
  readonly messageBubbles: Locator;
  readonly chatInputTextBox: Locator;
  readonly submitQueryBtn: Locator;
  readonly loadingIndicator: Locator;
  readonly startOverBtn: Locator;
  readonly hideChatBtn: Locator;

  // Results (left panel)
  readonly resultsBanner: Locator;
  readonly emptySearchPrompt: Locator;
  readonly nonZeroResultsPagingText: Locator;
  readonly results: Locator;

  // First result card
  readonly firstResult: Locator;
  readonly firstResultStatusBadge: Locator;
  readonly firstResultTitle: Locator;
  readonly firstResultTitleLink: Locator;
  readonly firstResultAuthor: Locator;
  readonly firstResultEdition: Locator;
  readonly firstResultPublisher: Locator;
  readonly firstResultPreviewBtn: Locator;

  constructor(page: Page) {
    this.page = page;

    // Authentication
    this.logInBtn = page.getByRole("link", { name: "Login" }); // update name for SCHOL-280

    // Chat interface (right panel)
    this.researchAssistantPanelHeading = page.getByRole("heading", {
      name: "Enhanced Search",
    });
    this.messageBubbles = page.getByTestId("assistant-message-bubble");
    this.chatInputTextBox = page.getByRole("textbox", {
      name: "Ask your question...",
    });
    this.submitQueryBtn = page.getByLabel("Send");
    this.loadingIndicator = page.getByText(
      "Thinking... This may take several seconds."
    );
    this.resultsBanner = page.getByText(
      "public domain scholarly e-books from our collections"
    );
    this.emptySearchPrompt = page.getByRole("heading", {
      name: "Start searching to see results",
    });
    this.startOverBtn = page.getByRole("button", { name: "Start over" });
    this.hideChatBtn = page.getByRole("button", { name: "Hide chat" });

    // Results (left panel)
    this.resultsBanner = page.getByText(
      "public domain scholarly e-books from our collections"
    );
    this.emptySearchPrompt = page.getByRole("heading", {
      name: "Start searching to see results",
    });
    this.nonZeroResultsPagingText = page.getByText(
      /\d+ - \d+ of \d+ results matching/
    );
    this.results = page.getByTestId(RESULT_TEST_ID);

    // First result card
    this.firstResult = this.results.first();
    this.firstResultStatusBadge = this.firstResult.getByTestId(
      "ds-statusBadge"
    );
    this.firstResultTitle = this.firstResult.getByTestId(RESULT_TITLE_TEST_ID);
    this.firstResultTitleLink = this.firstResultTitle.getByRole("link");
    this.firstResultAuthor = this.firstResult.getByTestId(
      RESULT_AUTHOR_TEST_ID
    );
    this.firstResultEdition = this.firstResult.getByTestId(
      RESULT_EDITION_TEST_ID
    );
    this.firstResultPublisher = this.firstResult.getByTestId(
      RESULT_PUBLISHER_TEST_ID
    );
    this.firstResultPreviewBtn = this.firstResult.getByRole("link", {
      name: "Preview",
    });
  }

  async navigateTo() {
    await this.page.goto("/research-assistant");
  }

  private static queryExecutionCount = 0;

  static getQueryExecutionCount() {
    return ResearchAssistantPage.queryExecutionCount;
  }

  async query(query: string) {
    await this.chatInputTextBox.fill(query);
    await new Promise((resolve) => setTimeout(resolve, 500)); // sleep for 0.5s to simulate user pause between typing and submitting
    await this.submitQueryBtn.click();
    ResearchAssistantPage.queryExecutionCount += 1;
  }

  // TODO: Relocate method to a base page class since the action can be carried out elsewhere
  async logIn(username: string, password: string) {
    // Ensure username and password are set (loaded from envars at runtime)
    if (!username || !password) {
      throw new Error("Username and password must be defined");
    }

    // If log in button is not visible, assume the user is already authenticated
    const shouldAttemptLogin = await this.logInBtn
      .isVisible()
      .catch(() => false);
    if (!shouldAttemptLogin) {
      return;
    }

    // Handle cases where the login page opens in a new tab or the same tab
    const newTabPromise = this.page
      .context()
      .waitForEvent("page")
      .catch(() => null);
    await this.logInBtn.click();
    const sameTabPromise = this.page
      .getByLabel("Username")
      .waitFor({ state: "visible" })
      .then(() => this.page)
      .catch(() => null);
    const loginPage =
      (await Promise.race([newTabPromise, sameTabPromise])) ?? this.page;

    // Fill in login form and submit
    await loginPage.getByLabel("Username").fill(username);
    await loginPage.getByLabel("Password").fill(password);
    await Promise.all([
      loginPage.waitForLoadState("networkidle"),
      loginPage.getByRole("button", { name: "Login" }).click(), // update name for SCHOL-280
    ]);
  }

  async getFirstResultEditionId(): Promise<string> {
    await this.firstResult.waitFor({ state: "visible" });
    const id = await this.firstResult
      .locator('[id^="edition-"]')
      .first()
      .getAttribute("id");
    return id.match(/^edition-(\d+)$/)[1];
  }
}

export { ResearchAssistantPage };
