import "@testing-library/jest-dom";

import { MatchMedia } from "@nypl/design-system-react-components";
new MatchMedia();

jest.mock("next/router", () => require("next-router-mock"));

window.scrollTo = jest.fn()
