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

  // Breadcrumb navigation
  readonly homeBreadcrumbLink: Locator;
  readonly researchBreadcrumbLink: Locator;
  readonly digitizedResearchBooksBreadcrumbLink: Locator;
  readonly enhancedSearchBreadcrumbLink: Locator;

  constructor(page: Page) {
    this.page = page;
    this.heroHeading = page.getByText("Try our new AI-enabled Enhanced Search");
    this.heroSubheading = page.getByText(
      "Find and discover content using natural language"
    );
    this.subNav = page.getByText("Enhanced SearchBETAKeyword search");
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
    this.learnMoreBtns = page
      .getByRole("link", { name: "Learn more" })
      .and(page.locator("#learn-more-button"));
    this.featuresSectionHeading = page.getByRole("heading", {
      name: "What can Enhanced Search help you do?",
    });
    this.accessSectionHeading = page.getByRole("heading", {
      name: "How does Enhanced Search work?",
    });
    this.faqSectionHeading = page.getByRole("heading", {
      name: "Frequently asked questions",
    });
    this.helpSectionHeading = page.getByRole("heading", {
      name: "Have more questions?",
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
    await this.page.goto("/research-assistant-landing");
  }

  async search(query: string) {
    await this.searchTextBox.fill(query);
    await this.searchSubmitBtn.click();
  }
}

export { ResearchAssistantLandingPage };
