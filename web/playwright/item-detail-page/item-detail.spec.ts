import { test } from "@playwright/test";
import { ItemDetailPage } from "./item-detail-page";

test.describe("Item Details Page Elements", () => {
  test("Navigate to an item details page and all item details page elements are displayed", async ({
    page,
  }) => {
    const itemDetailPage = new ItemDetailPage(page);
    await itemDetailPage.navigateToItemDetailPage();
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.searchCategoryDropdown
    );
    await itemDetailPage.verifyElementVisible(itemDetailPage.homepageSearchBox);
    await itemDetailPage.verifyElementVisible(itemDetailPage.searchButton);
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.advancedSearchLink
    );
    await itemDetailPage.verifyElementVisible(itemDetailPage.itemTitle);
    await itemDetailPage.verifyElementVisible(itemDetailPage.itemAuthor);
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemFeaturedEditionHeading
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemFeaturedEditionCover
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemFeaturedEditionYear
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemFeaturedEditionPublisher
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemFeaturedEditionLanguage
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemFeaturedEditionLicense
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemDetailsHeading
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemDetailsAuthorsHeading
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemDetailsAuthors
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemDetailsSubjectsHeading
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemDetailsSubjects
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemDetailsLanguagesHeading
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemDetailsLanguages
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemAllEditionsHeading
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemsCurrentlyAvailableOnlineToggleText
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.itemsCurrentlyAvailableOnlineToggle
    );
    await itemDetailPage.verifyElementVisible(itemDetailPage.secondItemEdition);
  });

  test("Click on 'show only items currently available online' button, only online available books should be displayed", async ({
    page,
  }) => {
    const itemDetailPage = new ItemDetailPage(page);
    await itemDetailPage.navigateToItemDetailPage();
    await itemDetailPage.clickElement(
      itemDetailPage.itemsCurrentlyAvailableOnlineToggle
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.firstReadOnlineButtonForAllEditions
    );
  });

  test("On the item details page for a collection the back to search results button is visible", async ({
    page,
  }) => {
    const itemDetailPage = new ItemDetailPage(page);
    await itemDetailPage.navigateToItemDetailPage();
    // await itemDetailPage.fillSearchBox("Jane Austen");
    await itemDetailPage.fillSearchBox("Finite mathematics");
    await itemDetailPage.clickSearchButton();
    // await itemDetailPage.clickElement(itemDetailPage.lettersOfJaneAustenLink);
    // await itemDetailPage.verifyElementVisible(
    //   itemDetailPage.lettersOfJaneAustenHeading
    // );
    await itemDetailPage.clickElement(itemDetailPage.finiteMathematicsLink);
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.finiteMathematicsHeading
    );
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.backToSearchResultsButton
    );
    await itemDetailPage.clickElement(itemDetailPage.backToSearchResultsButton);
    await itemDetailPage.verifyElementVisible(
      itemDetailPage.finiteMathematicsLink
    );
  });
});
