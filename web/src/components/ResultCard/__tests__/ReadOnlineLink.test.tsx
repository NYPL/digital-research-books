import { render, screen } from "@testing-library/react";
import { ResultPageProvider } from "~/src/context/ResultPageContext";
import { PageType } from "~/src/types/ResearchAssistant";
import ReadOnlineLink from "../Ctas/ReadOnlineLink";

const baseProps = {
  authors: ["Author"],
  title: "Test Title",
  isLoggedIn: true,
  loginCookie: {},
  readOnlineLink: {
    link_id: 1,
    mediaType: "text/html",
    url: "http://example.com/read",
    flags: {
      catalog: false,
      download: false,
      reader: false,
    },
  },
};

const providerValue = { page: "vra" as PageType, onReadOnline: jest.fn() };

describe("ReadOnlineLink", () => {
  test("renders nothing if readOnlineLink is missing", () => {
    const { container } = render(
      <ResultPageProvider value={{ page: "vra", onReadOnline: jest.fn() }}>
        <ReadOnlineLink {...baseProps} readOnlineLink={null} />
      </ResultPageProvider>
    );
    expect(container).toBeEmptyDOMElement();
  });

  test("renders button if embed flag is true", () => {
    render(
      <ResultPageProvider value={providerValue}>
        <ReadOnlineLink
          {...baseProps}
          readOnlineLink={{
            ...baseProps.readOnlineLink,
            flags: { ...baseProps.readOnlineLink.flags, embed: true },
          }}
        />
      </ResultPageProvider>
    );
    expect(
      screen.getByRole("button", { name: /Read online/ })
    ).toBeInTheDocument();
  });

  test("renders button in VRA context", () => {
    render(
      <ResultPageProvider value={providerValue}>
        <ReadOnlineLink {...baseProps} />
      </ResultPageProvider>
    );
    expect(
      screen.getByRole("button", { name: /Read online/ })
    ).toBeInTheDocument();
  });

  test("renders login link if login required and not logged in", () => {
    render(
      <ResultPageProvider value={providerValue}>
        <ReadOnlineLink
          {...baseProps}
          isLoggedIn={false}
          readOnlineLink={{
            ...baseProps.readOnlineLink,
            mediaType: baseProps.readOnlineLink.mediaType ?? "text/html",
            flags: {
              nypl_login: true,
              catalog: false,
              download: false,
              reader: false,
            },
          }}
        />
      </ResultPageProvider>
    );
    expect(screen.getByText(/Log in to read online/i)).toBeInTheDocument();
  });

  test("calls onReadOnline when button is clicked", () => {
    const onReadOnline = jest.fn();
    render(
      <ResultPageProvider value={{ ...providerValue, onReadOnline }}>
        <ReadOnlineLink {...baseProps} />
      </ResultPageProvider>
    );
    screen.getByRole("button", { name: /Read online/ }).click();
    expect(onReadOnline).toHaveBeenCalled();
  });
});
