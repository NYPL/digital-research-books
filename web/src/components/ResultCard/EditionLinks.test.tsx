import React from "react";
import { render, screen } from "@testing-library/react";
import EditionLinks from "./EditionLinks";

const work = {
    editions: [
        { edition_id: 1, publication_date: "2000" },
        { edition_id: 2, publication_date: "2010" },
        { edition_id: 3, publication_date: "2020" },
    ],
};

describe("EditionLinks", () => {
    test("renders edition links if more than one edition", () => {
        render(<EditionLinks work={work} />);
        expect(screen.getAllByRole("link").length).toBe(2);
    });

    test("renders nothing if only one edition", () => {
        render(<EditionLinks work={{ editions: [{ edition_id: 1 }] }} />);
        expect(screen.queryByRole("link")).not.toBeInTheDocument();
    });
});
