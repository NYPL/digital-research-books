import { Locator, Page } from "@playwright/test";

const ITEM_PAGE_URL =
  "/item/9e6a33c8-eee0-498c-821c-f19e4ac70660?featured=15251797";

class ItemPage {
  readonly page: Page;

  // Authentication
  readonly logInBtn: Locator;

  // Sidebar — core metadata
  readonly eBookLabel: Locator;
  readonly pageTitle: Locator;
  readonly authorLink: Locator;
  readonly downloadPdfBtn: Locator;

  // Sidebar — accordion triggers
  readonly detailsAccordion: Locator;
  readonly whatIsThisBookAboutAccordion: Locator;
  readonly downloadOptionsAccordion: Locator;
  readonly searchInsideAccordion: Locator;
  readonly otherEditionsAccordion: Locator;
  readonly relatedBooksAccordion: Locator;

  // PDF reader
  readonly readerControls: Locator;
  readonly readerContent: Locator;
  readonly noPdfDataMessage: Locator;

  // Chat panel
  readonly chatPanelHeading: Locator;
  readonly messageBubbles: Locator;
  readonly chatInputTextBox: Locator;
  readonly submitQueryBtn: Locator;
  readonly loadingIndicator: Locator;

  // Details panel — labels
  readonly detailsCopyrightLabel: Locator;
  readonly detailsEditionLabel: Locator;
  readonly detailsPublisherLabel: Locator;
  readonly detailsPlaceOfPublicationLabel: Locator;
  readonly detailsSubjectsLabel: Locator;
  readonly detailsLanguagesLabel: Locator;

  // Details panel — values
  readonly detailsCopyrightValue: Locator;
  readonly detailsEditionValue: Locator;
  readonly detailsPublisherValue: Locator;
  readonly detailsPlaceOfPublicationValue: Locator;
  readonly detailsSubjectsValue: Locator;
  readonly detailsLanguagesValue: Locator;

  constructor(page: Page) {
    this.page = page;

    // Sidebar — core metadata
    this.eBookLabel = page.getByText("E-BOOK");
    this.pageTitle = page.getByRole("heading", { level: 1 });
    this.authorLink = page.locator('a[href*="author"]').first();
    this.downloadPdfBtn = page.getByRole("link", { name: "Download PDF" });

    // Sidebar accordion triggers
    this.detailsAccordion = page.getByRole("button", { name: "Details" });
    this.whatIsThisBookAboutAccordion = page.getByRole("button", {
      name: "What is this book about?",
    });
    this.downloadOptionsAccordion = page.getByRole("button", {
      name: "Download options",
    });
    this.searchInsideAccordion = page.getByRole("button", {
      name: "Search inside this book",
    });
    this.otherEditionsAccordion = page.getByRole("button", {
      name: "Other editions",
    });
    this.relatedBooksAccordion = page.getByRole("button", {
      name: "Related books",
    });

    // Authentication
    this.logInBtn = page.getByRole("link", { name: "Login" });

    // PDF reader controls
    this.readerControls = page.getByRole("region", {
      name: "Reader controls",
    });
    this.readerContent = page.getByRole("region", { name: "Reader content" });

    // Chat panel
    this.chatPanelHeading = page.getByRole("heading", {
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

    // Details panel — labels
    this.detailsCopyrightLabel = page.getByText("Copyright", { exact: true });
    this.detailsEditionLabel = page.getByText("Edition", { exact: true });
    this.detailsPublisherLabel = page.getByText("Publisher", { exact: true });
    this.detailsPlaceOfPublicationLabel = page.getByText(
      "Place of publication",
      { exact: true }
    );
    this.detailsSubjectsLabel = page.getByText("Subjects", { exact: true });
    this.detailsLanguagesLabel = page.getByText("Languages", { exact: true });

    // Details panel — values
    // Scope to the details accordion panel region, then traverse from each
    // label to its sibling value via the shared parent container.
    const detailsPanel = page.getByRole("region", { name: "Details" });

    this.detailsCopyrightValue = detailsPanel
      .getByText("Copyright", { exact: true })
      .locator("..")
      .getByTestId("ds-link");

    this.detailsEditionValue = detailsPanel
      .getByText("Edition", { exact: true })
      .locator("..")
      .getByTestId("ds-text")
      .last();

    this.detailsPublisherValue = detailsPanel
      .getByText("Publisher", { exact: true })
      .locator("..")
      .getByTestId("ds-text")
      .last();

    this.detailsPlaceOfPublicationValue = detailsPanel
      .getByText("Place of publication", { exact: true })
      .locator("..")
      .getByTestId("ds-text")
      .last();

    this.detailsSubjectsValue = detailsPanel
      .getByText("Subjects", { exact: true })
      .locator("..")
      .getByTestId("ds-list");

    this.detailsLanguagesValue = detailsPanel
      .getByText("Languages", { exact: true })
      .locator("..")
      .getByTestId("ds-text")
      .last();
  }

  async navigateTo() {
    await this.page.goto(ITEM_PAGE_URL);
  }

  // TODO: Move to a utility class to be used across multiple pages
  async query(text: string) {
    await this.chatInputTextBox.fill(text);
    await new Promise((resolve) => setTimeout(resolve, 500)); // sleep for 0.5s to simulate user pause between typing and submitting
    await this.submitQueryBtn.click();
  }

  async logIn(username: string, password: string) {
    if (!username || !password) {
      throw new Error("Username and password must be defined");
    }

    const shouldAttemptLogin = await this.logInBtn
      .isVisible()
      .catch(() => false);
    if (!shouldAttemptLogin) {
      return;
    }

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

    await loginPage.getByLabel("Username").fill(username);
    await loginPage.getByLabel("Password").fill(password);
    await Promise.all([
      loginPage.waitForLoadState("networkidle"),
      loginPage.getByRole("button", { name: "Login" }).click(),
    ]);
  }
}

export { ItemPage };
