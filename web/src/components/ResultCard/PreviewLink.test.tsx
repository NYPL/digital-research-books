import React from "react";
import { render, screen } from "@testing-library/react";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import PreviewLink from "./PreviewLink";
import { PageType } from "~/src/types/ResearchAssistant";

const previewLink = {
    link_id: 1,
    mediaType: "text/html",
    url: "preview",
    flags: {
        catalog: false,
        download: false,
        reader: false,
    },
};
const workId = "work1";
const editionId = 123;

const renderWithProvider = (page: PageType) =>
    render(
        <ResultPageProvider value={{ page, onReadOnline: jest.fn() }}>
            <PreviewLink
                previewLink={previewLink}
                workId={workId}
                editionId={editionId}
            />
        </ResultPageProvider>
    );

describe("PreviewLink", () => {
    test("renders preview button in VRA context", () => {
        renderWithProvider("vra");
        expect(
            screen.getByRole("link", { name: /Preview item/i })
        ).toBeInTheDocument();
    });

    test("renders not available if not VRA context", () => {
        renderWithProvider("keyword");
        expect(screen.getByText(/Not yet available/i)).toBeInTheDocument();
    });
});
