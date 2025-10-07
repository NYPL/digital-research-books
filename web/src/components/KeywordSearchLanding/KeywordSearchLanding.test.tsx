import React from "react";
import { screen } from "@testing-library/react";
import { render } from "~/src/__tests__/testUtils/render";
import KeywordSearchLanding from "./KeywordSearchLanding";

describe("Renders keyword search landing", () => {
    beforeEach(async () => {
        render(<KeywordSearchLanding />);
    });
    test("Shows start searching text", () => {
        expect(
            screen.getByRole("heading", { name: "Start searching to see results from over 1 million scholarly e-books in the public domain" })
        ).toBeInTheDocument();
    });
});
