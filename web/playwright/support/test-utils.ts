/* eslint-disable no-empty-pattern */
import { test as base, expect } from "@playwright/test";
import { http } from "msw";

const test = base.extend<{
  setCookie(expires?: number): Promise<void>;
  http: typeof http;
}>({
  setCookie: [
    async ({ context }, use, _expires) => {
      async function addCookie(
        expires: number = (Date.now() + 60 * 60 * 24 * 1000) / 1000
      ) {
        const cookie = {
          name: "nyplIdentityPatron",
          value: JSON.stringify({
            token_type: "Bearer",
            access_token: "access-token",
            refresh_token: "refresh-token",
            expires: expires,
          }),
          domain: "localhost",
          path: "/",
          expires: expires,
        };
        await context.addCookies([cookie]);
      }
      addCookie();
      await use(addCookie);
    },
    { auto: true },
  ],
  http,
});

export { expect, test };

