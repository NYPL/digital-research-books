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

  // Breadcrumb navigation
  readonly homeBreadcrumbLink: Locator;
  readonly researchBreadcrumbLink: Locator;
  readonly digitizedResearchBooksBreadcrumbLink: Locator;
  readonly enhancedSearchBreadcrumbLink: Locator;

  // First result card
  readonly firstResult: Locator;
  readonly firstResultStatusBadge: Locator;
  readonly firstResultTitle: Locator;
  readonly firstResultTitleLink: Locator;
  readonly firstResultAuthor: Locator;
  readonly firstResultEdition: Locator;
  readonly firstResultPublisher: Locator;
  readonly firstResultReadOnlineBtn: Locator;

  constructor(page: Page) {
    this.page = page;

    // Chat interface (right panel)
    this.researchAssistantPanelHeading = page.getByRole("heading", {
      name: /^Enhanced Search$/,
    });
    this.messageBubbles = page.getByTestId("assistant-message-bubble");
    this.chatInputTextBox = page.getByRole("textbox", {
      name: "Ask your question...",
    });
    this.submitQueryBtn = page.getByLabel("Send");
    this.loadingIndicator = page
      .getByLabel("Chat messages")
      .getByText("Thinking... This may take several seconds.");
    this.resultsBanner = page.getByText(
      "This tool only searches the Digitized Research Books collection"
    );
    this.emptySearchPrompt = page.getByRole("heading", {
      name: "Start searching to see results",
    });
    this.startOverBtn = page.getByRole("button", { name: "Start over" });
    this.hideChatBtn = page.getByRole("button", { name: "Hide chat" });

    // Results (left panel)
    this.resultsBanner = page.getByText(
      "This tool only searches the Digitized Research Books collection"
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
    this.firstResultReadOnlineBtn = this.firstResult.getByRole("link", {
      name: "Read online",
    });

    // Breadcrumb navigation
    const breadcrumbNav = page.getByRole("navigation", { name: "Breadcrumb" });
    this.homeBreadcrumbLink = breadcrumbNav.getByRole("link", { name: "Home" });
    this.researchBreadcrumbLink = breadcrumbNav.getByRole("link", {
      name: "Research",
    });
    this.digitizedResearchBooksBreadcrumbLink = breadcrumbNav.getByRole(
      "link",
      { name: "Digitized Research Books" }
    );
    this.enhancedSearchBreadcrumbLink = breadcrumbNav.getByRole("link", {
      name: "Enhanced Search (beta)",
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
