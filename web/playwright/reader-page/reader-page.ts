import { expect, Locator, Page } from "@playwright/test";

class ReaderPage {
  readonly page: Page;
  readonly homepageSearchBox: Locator;
  readonly searchButton: Locator;
  readonly robotSoccerTitle: Locator;
  readonly firstReadOnlineButton: Locator;

  constructor(page: Page) {
    this.page = page;
    this.homepageSearchBox = page.locator("[aria-label='Item Search']");
    this.searchButton = page.locator("#search-bar-submit-button");
    this.robotSoccerTitle = page.locator("//a[text()='Robot soccer'] >> nth=0");
    this.firstReadOnlineButton = page.locator("a:text('Read Online') >> nth=0");
  }

  async navigateToHome() {
    await this.page.goto("/");
  }

  async navigateToReaderPage() {
    await this.page.goto("/read/4440666");
  }

  async fillSearchBox(query: string) {
    await this.homepageSearchBox.fill(query);
  }

  async clickSearchButton() {
    await this.searchButton.click();
  }

  async verifyRobotSoccerTitleVisible() {
    await expect(this.robotSoccerTitle).toBeVisible();
  }

  async verifyFirstReadOnlineButtonVisible() {
    await expect(this.firstReadOnlineButton).toBeVisible();
  }
}

export { ReaderPage };
