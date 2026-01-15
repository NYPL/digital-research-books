import "@testing-library/jest-dom";

import { MatchMedia } from "@nypl/design-system-react-components";
new MatchMedia();

jest.mock("next/router", () => require("next-router-mock"));

window.scrollTo = jest.fn();
global.fetch = jest.fn(() =>
  Promise.resolve({
    ok: true,
    json: async () => ({}),
  })
) as jest.Mock;

// Suppress specific React warnings in tests
const originalError = console.error;
beforeAll(() => {
  console.error = (...args: any[]) => {
    if (
      typeof args[0] === "string" &&
      args[0].includes(
        "Use the `defaultValue` or `value` props instead of setting children on <textarea>"
      )
    ) {
      return;
    }
    originalError.call(console, ...args);
  };
});

afterAll(() => {
  console.error = originalError;
});

beforeEach(() => {
  (global.fetch as jest.Mock).mockClear();
});
