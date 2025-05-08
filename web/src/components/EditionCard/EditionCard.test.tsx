import React from "react";
import { EditionCard } from "./EditionCard";
import { screen } from "@testing-library/react";
import { PLACEHOLDER_COVER_LINK } from "~/src/constants/editioncard";
import {
  eddEdition,
  fullEdition,
  upEdition,
} from "~/src/__tests__/fixtures/EditionCardFixture";
import { NYPL_SESSION_ID } from "~/src/constants/auth";
import { Cookies, CookiesProvider } from "react-cookie";
import { render } from "~/src/__tests__/testUtils/render";

describe("Edition Card with Valid Data", () => {
  beforeEach(() => {
    render(<EditionCard authors={[]} edition={fullEdition} title={"title"}></EditionCard>);
  });
  test("Shows Year as Link in header", () => {
    expect(screen.getByText("1990 Edition").closest("a").href).toContain(
      "/edition/12345"
    );
  });
  test("Shows Full Publisher", () => {
    expect(
      screen.getByText(
        "Published in Chargoggagoggmanchauggagoggchaubunagungamaugg by publisher_1"
      )
    ).toBeInTheDocument();
  });
  test("Shows Full list of languages", () => {
    expect(
      screen.getByText(
        "Languages: english, french, russian, unknown, spanish, german, arabic, hindi, japanese, vietnamese, latin, romanian"
      )
    ).toBeInTheDocument();
  });
  test("Shows license with links", () => {
    expect(
      screen.getByText("Copyright: test rights statement").closest("a").href
    ).toContain("/copyright");
  });
  test("Shows cover", () => {
    expect(
      screen.getByAltText("Cover for 1990 Edition").closest("img").src
    ).toEqual("https://test-cover/");
  });
  test("Shows download as link", () => {
    expect(screen.getByText("Download PDF").closest("a").href).toEqual(
      "https://test-link-url-2/"
    );
  });
  test("Shows 'read online' as link", () => {
    expect(screen.getByText("Read Online").closest("a").href).toContain(
      "/read/12"
    );
  });
});

describe("Edition Year with Minimal Data", () => {
  beforeEach(() => {
    render(
      <EditionCard
        authors={[]}
        edition={{ edition_id: 54321 }}
        title={"title"}
      ></EditionCard>
    );
  });
  test("Shows Unknown Year as Link in header", () => {
    expect(
      screen.getByText("Edition Year Unknown").closest("a").href
    ).toContain("/edition/54321");
  });
  test("Shows Unknown Publisher", () => {
    expect(
      screen.getByText("Publisher and Location Unknown")
    ).toBeInTheDocument();
  });
  test("Shows Unknown languages", () => {
    expect(screen.getByText("Languages: Undetermined")).toBeInTheDocument();
  });
  test("Shows Unknown license with links", () => {
    expect(screen.getByText("Copyright: Unknown").closest("a").href).toContain(
      "/copyright"
    );
  });
  test("Shows Placeholder cover", () => {
    expect(screen.getByAltText("Placeholder Cover").closest("img").src).toEqual(
      PLACEHOLDER_COVER_LINK
    );
  });
  test("Not available ctas", () => {
    expect(screen.getByText("Not yet available")).toBeInTheDocument();
    expect(screen.queryByText("Download PDF")).not.toBeInTheDocument();
    expect(screen.queryByText("Read Online")).not.toBeInTheDocument();
  });
});

describe("Edition with EDD", () => {
  test("Shows Download and Read Online button when edition has both EDD and readable links", () => {
    render(<EditionCard authors={[]} edition={fullEdition} title={"title"}></EditionCard>);

    expect(screen.getByText("Download PDF")).toBeInTheDocument();
    expect(screen.getByText("Read Online")).toBeInTheDocument();
    expect(
      screen.queryByText("Log in to request scan")
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Request Scan")).not.toBeInTheDocument();
  });

  test("Shows Login button when EDD is available but user is not logged in", () => {
    render(<EditionCard authors={[]} edition={eddEdition} title={"title"}></EditionCard>);
    expect(
      screen.getByRole("link", { name: "Log in to request scan for title" })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Log in to request scan for title" })
    ).toHaveAttribute(
      "href",
      expect.stringContaining("https://login.nypl.org/auth/login")
    );
    expect(screen.queryByText("Download PDF")).not.toBeInTheDocument();
    expect(screen.queryByText("Read Online")).not.toBeInTheDocument();
  });

  test("Shows EDD Request button when user is logged in", () => {
    // Set cookie before rendering the component
    const cookies = new Cookies();
    cookies.set(NYPL_SESSION_ID, "randomvalue");
    render(
      <CookiesProvider cookies={cookies}>
        <EditionCard authors={[]} edition={eddEdition} title={"title"} />
      </CookiesProvider>
    );

    expect(
      screen.getByRole("link", { name: "Request scan for title" })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Request scan for title" })
    ).toHaveAttribute("href", expect.stringContaining("test-link-url"));
    expect(screen.queryByText("Download PDF")).not.toBeInTheDocument();
    expect(screen.queryByText("Read Online")).not.toBeInTheDocument();
  });

  test("Shows 'Physical Edition' badge and 'Scan and Deliver' link", () => {
    render(<EditionCard authors={[]} edition={eddEdition} title={"title"} />);
    expect(screen.getByText("Physical Edition")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Scan and Deliver" })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Scan and Deliver" })
    ).toHaveAttribute("href", "https://www.nypl.org/research/scan-and-deliver");
  });
});

describe("Edition with UP", () => {
  test("Shows Login button when user is not logged in", () => {
    render(<EditionCard authors={[]} edition={upEdition} title={"title"}></EditionCard>);
    expect(
      screen.getByRole("link", { name: "title Log in to read online" })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "title Log in to read online" })
    ).toHaveAttribute(
      "href",
      expect.stringContaining("https://login.nypl.org/auth/login")
    );
    expect(screen.queryByText("Download PDF")).not.toBeInTheDocument();
    expect(screen.queryByText("Read Online")).not.toBeInTheDocument();
  });
  test("Shows 'Library Card Required' badge", () => {
    render(<EditionCard authors={[]} edition={upEdition} title={"title"} />);
    expect(screen.getByText("LIBRARY CARD REQUIRED")).toBeInTheDocument();
  });
  test("Shows Read Online and Download buttons when user is logged in", () => {
    // Set cookie before rendering the component
    const cookies = new Cookies();
    cookies.set(NYPL_SESSION_ID, "randomvalue");
    render(
      <CookiesProvider cookies={cookies}>
        <EditionCard authors={[]} edition={upEdition} title={"title"} />
      </CookiesProvider>
    );

    expect(
      screen.getByRole("link", { name: "title Read Online" })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "title Read Online" })
    ).toHaveAttribute("href", expect.stringContaining("/read/12"));
    expect(
      screen.getByRole("link", { name: "title Download PDF" })
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "title Download PDF" })
    ).toHaveAttribute("href", expect.stringContaining("test-link-url"));
  });
  test("Shows blurb with publisher", () => {
    render(<EditionCard authors={[]} edition={upEdition} title={"title"}></EditionCard>);
    expect(
      screen.getByText("Digitalized by NYPL with permission of publisher_1")
    ).toBeInTheDocument();
  });
});
