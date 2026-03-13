import { render, screen } from "@testing-library/react";
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

describe("ReadOnlineLink", () => {
  test("renders nothing if readOnlineLink is missing", () => {
    const { container } = render(
      <ReadOnlineLink {...baseProps} readOnlineLink={null} />
    );
    expect(container).toBeEmptyDOMElement();
  });

  test("renders button if embed flag is true", () => {
    render(
      <ReadOnlineLink
        {...baseProps}
        readOnlineLink={{
          ...baseProps.readOnlineLink,
          flags: { ...baseProps.readOnlineLink.flags, embed: true },
        }}
      />
    );
    expect(
      screen.getByRole("button", { name: /Read online/ })
    ).toBeInTheDocument();
  });

  test("renders button in VRA context", () => {
    render(<ReadOnlineLink {...baseProps} />);
    expect(
      screen.getByRole("button", { name: /Read online/ })
    ).toBeInTheDocument();
  });

  test("renders login link if login required and not logged in", () => {
    render(
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
    );
    expect(screen.getByText(/Log in to read online/i)).toBeInTheDocument();
  });
});
