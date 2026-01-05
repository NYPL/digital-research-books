import { Locator, Page } from "@playwright/test";

class ResearchAssistantLandingPage {
  readonly page: Page;
  readonly heroHeading: Locator;
  readonly heroSubheading: Locator;
  readonly subNav: Locator;
  readonly searchTextBox: Locator;
  readonly searchSubmitBtn: Locator;
  readonly suggestBtnRomanEmpire: Locator;
  readonly suggestBtnFeminism: Locator;
  readonly suggestBtnMethodistChurch: Locator;
  readonly suggestBtnAmericanCivilWar: Locator;
  readonly learnMoreBtn: Locator;
  readonly featuresSectionHeading: Locator;
  readonly accessSectionHeading: Locator;
  readonly focusedResearchSectionHeading: Locator;
  readonly faqSectionHeading: Locator;
  readonly helpSectionHeading: Locator;

  constructor(page: Page) {
    this.page = page;
    this.heroHeading = page.getByText("NYPL Virtual Research Assistant");
    this.heroSubheading = page.getByText("Your AI partner in discovering relevant research");
    this.subNav = page.getByText('Virtual Research AssistantBETAKeyword search')
    this.searchTextBox = page.getByRole("textbox", { name: "What research topic"});
    this.searchSubmitBtn = page.locator("[aria-label='Send']");
    this.suggestBtnRomanEmpire = page.getByRole("button", { name: "the Roman Empire" });
    this.suggestBtnFeminism = page.getByRole("button", { name: "feminism in medieval times" });
    this.suggestBtnMethodistChurch = page.getByRole("button", { name: "the Methodist Church" });
    this.suggestBtnAmericanCivilWar = page.getByRole("button", { name: "American Civil War" });
    this.learnMoreBtn = page.getByRole("button", { name: "Learn more ↓" })
    this.featuresSectionHeading = page.getByRole(
      "heading",
      { name: "Get more out of your research journey with the power of AI" }
    );
    this.accessSectionHeading = page.getByRole(
      "heading",
      { name: "Access and engage with scholarly e-books in minutes" }
    );
    this.focusedResearchSectionHeading = page.getByRole(
      "heading",
      { name: "Do focused research with smart tools" }
    );
    this.faqSectionHeading = page.getByRole("heading", { name: "Frequently asked questions" });
    this.helpSectionHeading = page.getByText(
      "Have a question? Get help or learn more about this project"
    );
  }

  async navigateTo() { await this.page.goto("/research-assistant-landing"); }

  async search(query: string) {
    await this.searchTextBox.fill(query);
    await this.searchSubmitBtn.click();
  }
}

export { ResearchAssistantLandingPage };
