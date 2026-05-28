import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import mockRouter from "next-router-mock";
import { render } from "~/src/__tests__/testUtils/render";
import { breadcrumbTitles, inputTerms } from "~/src/constants/labels";
import {
  workDetail as apiWork,
  workDetailWithCatalog as apiWorkOnlyCatalog,
  workDetailInCollection,
} from "../../__tests__/fixtures/WorkDetailFixture";
import Work from "./Work";

const backUrl = "/search?query=keyword%3AYoruba&sort=title%3ADESC";

describe("Renders Work component when given valid work", () => {
  beforeEach(() => {
    render(<Work workResult={apiWork} />);
  });
  test("Digital Research Books Beta doesn't have href attribute", () => {
    const homepagelinks = screen.getAllByText("Digital Research Books Beta");
    homepagelinks.forEach((link) => {
      expect(link).not.toHaveAttribute("href");
    });
  });
  test("Shows Header with Searchbar", () => {
    expect(
      screen.getByRole("heading", { name: breadcrumbTitles.drb })
    ).toBeInTheDocument();
    expect(screen.getByRole("combobox")).toHaveValue(inputTerms[0].value);
    expect(screen.getByRole("textbox")).toBeInTheDocument();
    expect(screen.getByText("Advanced Search").closest("a").href).toContain(
      "/advanced-search"
    );
  });
  test("Shows Work Title in Heading", () => {
    expect(
      screen.getByRole("heading", { name: "Yoruba; intermediate texts" })
    ).toBeInTheDocument();
  });
  test("Shows Work Subtitle", () => {
    expect(screen.getByText("sub title sub title")).toBeInTheDocument();
  });
  test("Shows Author name twice, both in links", () => {
    const authorElements = screen.getAllByText("McClure, H. David. ()", {
      exact: false,
    });
    expect(authorElements.length).toEqual(2);
    authorElements.forEach((elem) => {
      expect(elem.closest("a").href).toContain(
        "query=author%3AMcClure%2C+H.+David.+%28%29"
      );
    });
  });

  test("Featured Edition Card shows up once in page", () => {
    expect(screen.getByText("FEATURED EDITION")).toBeInTheDocument();
    const featuredEditionHeadings = screen.getAllByRole("heading", {
      name: "1967 Edition",
    });
    expect(featuredEditionHeadings.length).toEqual(1);
    featuredEditionHeadings.forEach((heading) => {
      expect(
        (within(heading).getByRole("link") as HTMLLinkElement).href
      ).toContain("/edition");
      expect(
        (within(heading).getByRole("link") as HTMLLinkElement).href
      ).toContain("?featured=1280883");
    });

    expect(screen.getAllByAltText("Cover for 1967 Edition").length).toBe(1);
    expect(
      screen.getAllByText(
        "Published by Foreign Service Institute, Dept. of State;"
      ).length
    ).toBe(1);
    expect(screen.getAllByText("Languages: English, German").length).toBe(1);
    expect(
      screen.getAllByText("Copyright: Unknown")[0].closest("a").href
    ).toContain("/copyright");
  });
  test("Shows Details Table", () => {
    expect(
      screen.getByRole("heading", { name: "Details" })
    ).toBeInTheDocument();
  });
  test("Does not show Part of Collection card when inCollections is empty", () => {
    expect(screen.queryByText("Part of Collection")).not.toBeInTheDocument();
  });
});

describe("Edition Cards and toggles", () => {
  describe("Work with no showAll query passed", () => {
    beforeEach(() => {
      render(<Work workResult={apiWork} />);
    });

    test("Edition Toggle defaults to checked", () => {
      const toggle = screen.getByLabelText(
        "Show only items currently available online"
      ) as HTMLInputElement;
      expect(toggle).toBeInTheDocument();
      expect(toggle).not.toBeChecked();
    });

    test("clicking the edition toggle sends a new query", async () => {
      const toggle = screen.getByLabelText(
        "Show only items currently available online"
      ) as HTMLInputElement;
      await userEvent.click(toggle);

      expect(mockRouter).toMatchObject({
        pathname: "/",
        query: { showAll: false },
      });
    });
  });

  describe("Work with showAll=true", () => {
    beforeEach(() => {
      mockRouter.push("?showAll=true");
      render(<Work workResult={apiWork} />);
    });

    test("Edition Toggle is unchecked", () => {
      const toggle = screen.getByLabelText(
        "Show only items currently available online"
      ) as HTMLInputElement;
      expect(toggle).toBeInTheDocument();
      expect(toggle).not.toBeChecked();
    });

    test("clicking the edition toggle sends a new query", async () => {
      const toggle = screen.getByLabelText(
        "Show only items currently available online"
      ) as HTMLInputElement;
      await userEvent.click(toggle);

      expect(mockRouter).toMatchObject({
        pathname: "/",
        query: { showAll: false },
      });
    });
  });

  describe("Work with showAll=false", () => {
    beforeEach(() => {
      mockRouter.push("?showAll=false");
      render(<Work workResult={apiWork} />);
    });

    test("Edition Toggle is checked", () => {
      const toggle = screen.getByLabelText(
        "Show only items currently available online"
      ) as HTMLInputElement;
      expect(toggle).toBeInTheDocument();
      expect(toggle).toBeChecked();
    });

    test("clicking the edition toggle sends a new query", async () => {
      const toggle = screen.getByLabelText(
        "Show only items currently available online"
      ) as HTMLInputElement;
      await userEvent.click(toggle);

      expect(mockRouter).toMatchObject({
        pathname: "/",
        query: { showAll: true },
      });
    });
  });

  describe("loading work with featured=862232", () => {
    beforeEach(() => {
      mockRouter.push("?featured=862232");
      render(<Work workResult={apiWork} />);
    });

    test("1980 edition shows up twice", () => {
      expect(screen.getByText("FEATURED EDITION")).toBeInTheDocument();
      const featuredEditionHeadings = screen.getAllByRole("heading", {
        name: "1980 Edition",
      });
      expect(featuredEditionHeadings.length).toEqual(1);
    });
  });
});

describe("Render work with only catalog edition available", () => {
  beforeEach(() => {
    render(<Work workResult={apiWorkOnlyCatalog} />);
  });

  test("Featured Edition does not show when only catalog edition is available", () => {
    expect(
      screen.queryByRole("heading", { name: "Featured Edition" })
    ).not.toBeInTheDocument();
  });
});

describe("Work - Back to search results link", () => {
  describe("Show back to search results link with backUrl provided", () => {
    beforeEach(() => {
      render(<Work workResult={apiWork} backUrl={backUrl} />);
    });

    test("Shows back to search results link", () => {
      expect(
        screen.getByRole("link", { name: "Back to search results" })
      ).toBeInTheDocument();
    });
    test("Back to search results links to search page", () => {
      expect(
        screen.getByRole("link", { name: "Back to search results" })
      ).toHaveAttribute(
        "href",
        "/search?query=keyword%3AYoruba&sort=title%3ADESC"
      );
    });
  });

  describe("Does not show back to search results link", () => {
    beforeEach(() => {
      render(<Work workResult={apiWork} />);
    });

    test("Does not show back to search results link", () => {
      expect(
        screen.queryByRole("link", { name: "Back to search results" })
      ).not.toBeInTheDocument();
    });
  });
});

describe("Renders Part of Collection card when inCollections is not empty", () => {
  beforeEach(() => {
    render(<Work workResult={workDetailInCollection} />);
  });

  test("Shows Part of Collection", () => {
    expect(screen.getByText("Part of Collection")).toBeInTheDocument();
  });

  test("Browse Collection link directs to collection page", () => {
    expect(
      screen.getByRole("link", {
        name: "Browse Collection",
      })
    ).toHaveAttribute(
      "href",
      "/collection/37a7e91d-31cd-444c-8e97-7f17426de7ec"
    );
  });
});
