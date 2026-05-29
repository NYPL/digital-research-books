import { BrowserContext, expect, Page, test } from "@playwright/test";
import { ItemPage } from "./item-page";

test.describe("Item Page UI", { tag: "@enhanced-search" }, () => {
  let page: Page;
  let itemPage: ItemPage;

  test.beforeAll(async ({ browser }) => {
    page = await browser.newPage();
    itemPage = new ItemPage(page);
    await itemPage.navigateTo();
  });

  test.afterAll(async () => {
    await page.close();
  });

  test.describe("Summary metadata and download option", () => {
    test("E-BOOK label is visible", async () => {
      await expect(itemPage.eBookLabel).toBeVisible();
    });

    test("Book title is visible", async () => {
      await expect(itemPage.pageTitle).toBeVisible();
    });

    test("Author link is visible", async () => {
      await expect(itemPage.authorLink).toBeVisible();
    });

    test("'Download PDF' button is visible", async () => {
      await expect(itemPage.downloadPdfBtn).toBeVisible();
    });
  });

  test.describe("Sidebar accordion panels", () => {
    test("'Details' accordion control is visible", async () => {
      await expect(itemPage.detailsAccordion).toBeVisible();
    });

    test("'What is this book about?' accordion control is visible", async () => {
      await expect(itemPage.whatIsThisBookAboutAccordion).toBeVisible();
    });

    test("'Download options' accordion control is visible", async () => {
      await expect(itemPage.downloadOptionsAccordion).toBeVisible();
    });

    test("'Search inside this book' accordion control is visible", async () => {
      await expect(itemPage.searchInsideAccordion).toBeVisible();
    });

    test("'Other editions' accordion control is visible", async () => {
      await expect(itemPage.otherEditionsAccordion).toBeVisible();
    });

    test("'Related books' accordion control is visible", async () => {
      await expect(itemPage.relatedBooksAccordion).toBeVisible();
    });
  });

  test.describe("PDF reader", () => {
    test("Reader controls region is visible", async () => {
      await expect(itemPage.readerControls).toBeVisible({ timeout: 60_000 });
    });

    test("Reader content is present", async () => {
      await expect(itemPage.readerContent).toBeAttached({ timeout: 60_000 });
    });
  });

  test.describe("Chat panel initial state", () => {
    test("Chat panel heading is visible", async () => {
      await expect(itemPage.chatPanelHeading).toBeVisible();
    });

    test("Initial assistant message bubble is visible", async () => {
      await expect(itemPage.messageBubbles.nth(0)).toBeVisible();
    });

    test("Chat input text box is visible", async () => {
      await expect(itemPage.chatInputTextBox).toBeVisible();
    });
  });

  test.describe("Details panel metadata", () => {
    test.describe.configure({ mode: "serial" });

    const detailsFields = [
      {
        name: "Copyright",
        label: () => itemPage.detailsCopyrightLabel,
        value: () => itemPage.detailsCopyrightValue,
      },
      {
        name: "Edition",
        label: () => itemPage.detailsEditionLabel,
        value: () => itemPage.detailsEditionValue,
      },
      {
        name: "Publisher",
        label: () => itemPage.detailsPublisherLabel,
        value: () => itemPage.detailsPublisherValue,
      },
      {
        name: "Place of publication",
        label: () => itemPage.detailsPlaceOfPublicationLabel,
        value: () => itemPage.detailsPlaceOfPublicationValue,
      },
      {
        name: "Subjects",
        label: () => itemPage.detailsSubjectsLabel,
        value: () => itemPage.detailsSubjectsValue,
      },
      {
        name: "Languages",
        label: () => itemPage.detailsLanguagesLabel,
        value: () => itemPage.detailsLanguagesValue,
      },
    ];

    test.beforeAll(async () => {
      const isExpanded = await itemPage.detailsAccordion.getAttribute(
        "aria-expanded"
      );
      if (isExpanded !== "true") {
        await itemPage.detailsAccordion.click();
      }
    });

    for (const { name, label, value } of detailsFields) {
      test(`'${name}' label is visible`, async () => {
        await expect(label()).toBeVisible();
      });

      test(`'${name}' value has rendered text`, async () => {
        await expect(value()).toHaveText(/\S+/);
      });
    }
  });
});

test.describe(
  "Item Page Chat Functionality",
  { tag: "@enhanced-search" },
  () => {
    test.describe.configure({ mode: "serial" });
    test.setTimeout(120_000); // Override global timeout — AI response may be slow

    let context: BrowserContext;
    let page: Page;
    let itemPage: ItemPage;
    const testQuery = "what are the main topics of this text?";

    test.beforeAll(async ({ browser }) => {
      context = await browser.newContext();
      page = await context.newPage();
      itemPage = new ItemPage(page);
      await itemPage.navigateTo();
      await itemPage.logIn(process.env.VRA_USERNAME, process.env.VRA_PASSWORD);
      await itemPage.navigateTo(); // Return to item page after auth redirect
    });

    test.afterAll(async () => {
      await context.close();
    });

    test("Chat input accepts the query text", async () => {
      await itemPage.chatInputTextBox.fill(testQuery);
      const inputValue = await itemPage.chatInputTextBox.inputValue();
      expect(inputValue).toBe(testQuery);
    });

    test("An assistant response is displayed after submitting text", async () => {
      await itemPage.query(testQuery);

      await test.step("Wait for loading indicator to disappear", async () => {
        await expect(itemPage.loadingIndicator).toBeHidden({ timeout: 90_000 });
      });

      await expect(itemPage.messageBubbles.nth(1)).toBeVisible({
        timeout: 10_000, // Assistant response should shortly follow loading indicator disappearing
      });
    });
  }
);
