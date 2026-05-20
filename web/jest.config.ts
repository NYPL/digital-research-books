import type { Config } from "jest";

const config: Config = {
  moduleFileExtensions: ["js", "ts", "tsx"],
  moduleNameMapper: {
    "^~(.*)$": "<rootDir>$1",
    // Handle CSS imports (with CSS modules)
    // https://jestjs.io/docs/webpack#mocking-css-modules
    "^.+\\.module\\.(css|sass|scss)$": "identity-obj-proxy",
    "react-markdown": "<rootDir>/mocks/react-markdown.tsx",
  },
  testPathIgnorePatterns: [
    "<rootDir>/.next/",
    "<rootDir>/node_modules/",
    "testUtils",
    "fixtures",
    "componentHelpers",
    "/playwright/",
  ],
  setupFilesAfterEnv: ["./jest.setup.ts"],
  resetMocks: true,
  testEnvironment: "jsdom",
};

export default config;
