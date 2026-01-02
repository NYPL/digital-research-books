import React from "react";
import { render, screen } from "@testing-library/react";
import EddLink from "./EddLink";

const eddLink = {
    url: "www.example.com/edd",
    mediaType: "application/pdf",
    flags: {
        catalog: false,
        download: false,
        reader: false,
    },
    link_id: 1,
};
const title = "Test";

const renderEddLink = (isLoggedIn: boolean) =>
    render(<EddLink eddLink={eddLink} isLoggedIn={isLoggedIn} title={title} />);

describe("EddLink", () => {
    test("renders request scan link if logged in", () => {
        renderEddLink(true);
        expect(
            screen.getByRole("link", { name: /Request scan/ })
        ).toBeInTheDocument();
    });

    test("renders login to request scan if not logged in", () => {
        renderEddLink(false);
        expect(
            screen.getByRole("link", { name: /Log in to request scan/i })
        ).toBeInTheDocument();
    });
});
