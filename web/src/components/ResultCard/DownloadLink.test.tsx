import React from "react";
import { render, screen, fireEvent } from "@testing-library/react";
import DownloadLink from "./DownloadLink";
import { fulfillFetcher } from "~/src/lib/api/SearchApi";

const baseProps = {
    authors: ["Author"],
    title: "Test Title",
    isLoggedIn: true,
    loginCookie: {},
};

jest.mock("~/src/lib/api/SearchApi", () => ({
    fulfillFetcher: jest.fn(),
}));

describe("DownloadLink", () => {
    test("renders nothing if downloadLink is missing", () => {
        const { container } = render(
            <DownloadLink {...baseProps} downloadLink={null} />
        );
        expect(container).toBeEmptyDOMElement();
    });

    test("renders login link if nypl_login flag is true and not logged in", () => {
        render(
            <DownloadLink
                {...baseProps}
                isLoggedIn={false}
                downloadLink={{
                    link_id: 1,
                    url: "http://example.com/file.pdf",
                    flags: {
                        nypl_login: true,
                        catalog: false,
                        download: false,
                        reader: false,
                    },
                    mediaType: "application/pdf",
                }}
            />
        );
        expect(screen.getByText(/Log in to download PDF/i)).toBeInTheDocument();
    });

    test("renders download link if logged in and url present", () => {
        render(
            <DownloadLink
                downloadLink={{
                    link_id: 2,
                    url: "http://example.com/file.pdf",
                    flags: {
                        catalog: false,
                        download: false,
                        reader: false,
                    },
                    mediaType: "application/pdf",
                }}
                authors={[]}
                title={""}
                isLoggedIn={false}
            />
        );
        expect(screen.getByText(/Download PDF/i)).toBeInTheDocument();
    });

    test("handles /fulfill/ download with error", async () => {
        (fulfillFetcher as jest.Mock).mockResolvedValueOnce("Some error");

        render(
            <DownloadLink
                downloadLink={{
                    link_id: 3,
                    url: "/fulfill/123",
                    flags: {
                        catalog: false,
                        download: false,
                        reader: false,
                    },
                    mediaType: "application/pdf",
                }}
                authors={[]}
                title={""}
                isLoggedIn={false}
            />
        );
        fireEvent.click(screen.getByText(/Download PDF/i));
        expect(await screen.findByText(/Some error/i)).toBeInTheDocument();
    });
});
