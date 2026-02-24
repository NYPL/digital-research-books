import {
  API_URL,
  FULFILL_PATH,
  LIMITED_ACCESS_EDITION_PATH,
  NYPL_LOGIN_URL,
} from "~/mocks/mockEnv";
import { server } from "~/mocks/server";
import { LOGIN_TO_READ_TEST_ID } from "~/src/constants/testIds";
import { expect, test } from "../support/test-utils";

test.beforeEach(async ({ context }) => {
  await context.clearCookies();
});
test.afterEach(() => server.resetHandlers());
test.afterAll(() => server.close());

// This test is skipped since we do not have test credentials for production
test.skip("Cookie authentication", () => {
  test("redirects to NYPL login page with no cookie", async ({ page }) => {
    await page.goto(`${LIMITED_ACCESS_EDITION_PATH}`);
    await page.getByTestId(LOGIN_TO_READ_TEST_ID).click();
    await page.waitForURL(/.*login.nypl.org.*/);
    const url = new URL(page.url());
    const service = url.searchParams.get("service");

    expect(service).toContain(NYPL_LOGIN_URL);
  });

  test("redirects to NYPL login page with expired cookie", async ({
    page,
    setCookie,
  }) => {
    const cookieExpiration = new Date("1970-01-01T00:00:00.000Z").getTime();
    setCookie(cookieExpiration);

    await page.goto(`${LIMITED_ACCESS_EDITION_PATH}`);
    await page.getByTestId(LOGIN_TO_READ_TEST_ID).click();
    await page.waitForURL(/.*login.nypl.org.*/);
    const url = new URL(page.url());
    const service = url.searchParams.get("service");

    expect(service).toContain(NYPL_LOGIN_URL);
  });

  // TODO: logging in from localhost does not work
  test.skip("redirects to download with valid auth cookie", async ({
    page,
    setCookie,
    context,
  }) => {
    await setCookie();
    const cookies = await context.cookies();
    const authCookie = cookies.find(
      (cookie) => cookie.name === "nyplIdentityPatron"
    );

    expect(authCookie.path).toBe("/");

    await page.goto(`${LIMITED_ACCESS_EDITION_PATH}`);

    const responsePromise = page.waitForResponse(`${API_URL}${FULFILL_PATH}`);
    await page.getByRole("link", { name: "title Download PDF" }).click();
    const response = await responsePromise;

    expect(response.status()).toBe(302);
  });
});
