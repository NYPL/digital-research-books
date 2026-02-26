import { test } from "@playwright/test";
import { ReaderPage } from "./reader-page";

// TODO: replace this test with a more comprehensive test that validates all the features of reader page.
// This test is currently skipped because the test does not cover features of the reader.
test.skip("e-Reader validation", () => {
  test("Validate all the features of e-Reader is displayed", async ({
    page,
  }) => {
    const readerPage = new ReaderPage(page);
    await readerPage.navigateToHome();
    await readerPage.fillSearchBox("Robot Soccer");
    await readerPage.clickSearchButton();
    await readerPage.verifyRobotSoccerTitleVisible();
    await readerPage.verifyFirstReadOnlineButtonVisible();
  });
});
