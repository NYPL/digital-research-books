import { render } from "../../__tests__/testUtils/render";
import { screen } from "@testing-library/react";
import VRALayout from "./VRALayout";
import React from "react";

describe("VRA Layout component", () => {
    beforeEach(() => {
        render(
            <VRALayout activePage="vra">
                <div>Text in layout body</div>
            </VRALayout>
        );
    });

    test("Digital Research Books Beta doesn't have href attribute", () => {
        const homepagelinks = screen.getAllByText("Digital Research Books Beta");
        homepagelinks.forEach((link) => {
            expect(link).not.toHaveAttribute("href");
        });
    });
    test("DRB Header is shown", () => {
        expect(
            screen.getByRole("heading", { name: "Digital Research Books" })
        ).toBeInTheDocument();
    });
    test("should have text in layout body", () => {
        const text = screen.getByText("Text in layout body");
        expect(text).toBeInTheDocument();
    });
});
