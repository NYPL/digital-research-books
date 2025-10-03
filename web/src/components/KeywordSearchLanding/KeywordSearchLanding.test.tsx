import React from "react";
import { screen } from "@testing-library/react";
import { render } from "~/src/__tests__/testUtils/render";
import KeywordSearchLanding from "./KeywordSearchLanding";

describe("Renders Index Page", () => {
    beforeEach(async () => {
        render(<KeywordSearchLanding />);

        await screen.findByRole("heading", {
            name: "Digital Research Books Beta",
        });
    });
    test("Current page breadcrumb doesn't have href attribute", () => {
        expect(screen.getByText("Digital Research Books Beta")).not.toHaveAttribute(
            "href"
        );
    });
    test("Shows Heading", () => {
        expect(
            screen.getByRole("heading", { name: "Digital Research Books Beta" })
        ).toBeInTheDocument();
    });
});
