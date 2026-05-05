import { Locator, Page } from "@playwright/test";

class ResearchAssistantLandingPage {
  readonly page: Page;
  readonly heroHeading: Locator;
  readonly heroSubheading: Locator;
  readonly subNav: Locator;
  readonly searchTextBox: Locator;
  readonly searchSubmitBtn: Locator;
  readonly suggestBtnAncientRome: Locator;
  readonly suggestButtonMedievalWarfare: Locator;
  readonly suggestBtnOrnithology: Locator;
  readonly suggestBtnShipbuilding: Locator;
  readonly learnMoreBtns: Locator;
  readonly featuresSectionHeading: Locator;
  readonly accessSectionHeading: Locator;
  readonly focusedResearchSectionHeading: Locator;
  readonly faqSectionHeading: Locator;
  readonly helpSectionHeading: Locator;

  constructor(page: Page) {
    this.page = page;
    this.heroHeading = page.getByText("NYPL Virtual Research Assistant");
    this.heroSubheading = page.getByText(
      "Your AI partner in discovering content from over"
    );
    this.subNav = page.getByText(
      "Virtual Research AssistantBETAKeyword search"
    );
    this.searchTextBox = page.getByRole("textbox", {
      name: "What research topic",
    });
    this.searchSubmitBtn = page.getByRole("button", { name: "Send" });
    this.suggestBtnAncientRome = page.getByRole("button", {
      name: "Political figures of Ancient Rome",
    });
    this.suggestButtonMedievalWarfare = page.getByRole("button", {
      name: "Medieval warfare in China",
    });
    this.suggestBtnOrnithology = page.getByRole("button", {
      name: "Ornithology in the nineteenth century",
    });
    this.suggestBtnShipbuilding = page.getByRole("button", {
      name: "The science of shipbuilding",
    });
    this.learnMoreBtns = page.getByRole("link", { name: "Learn more" });
    this.featuresSectionHeading = page.getByRole("heading", {
      name: "What can the Assistant help you do?",
    });
    this.accessSectionHeading = page.getByRole("heading", {
      name: "How does the Assistant work?",
    });
    this.faqSectionHeading = page.getByRole("heading", {
      name: "Frequently asked questions",
    });
    this.helpSectionHeading = page.getByRole("heading", {
      name: "Have more questions?",
    });
  }

  async navigateTo() {
    await this.page.goto("/research-assistant-landing");
  }

  async search(query: string) {
    await this.searchTextBox.fill(query);
    await this.searchSubmitBtn.click();
  }
}

export { ResearchAssistantLandingPage };
