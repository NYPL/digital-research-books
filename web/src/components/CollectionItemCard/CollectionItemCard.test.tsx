import React from "react";
import { screen } from "@testing-library/react";
import { CollectionItemCard } from "./CollectionItemCard";
import { collectionItem } from "~/src/__tests__/fixtures/CollectionFixture";
import { render } from "~/src/__tests__/testUtils/render";

describe("Collection Item Card", () => {
  beforeEach(() => {
    render(<CollectionItemCard collectionItem={collectionItem} />);
  });
  test("Shows year as header", () => {
    expect(
      screen.getByRole("heading", { name: "1900 Edition" })
    ).toBeInTheDocument();
  });
  test("Shows full publisher", () => {
    expect(
      screen.getByText(
        "Published in New York (State) by American Sports Publishing,"
      )
    ).toBeInTheDocument();
  });
  test("Shows Read Online button", () => {
    expect(screen.getByText("Read Online")).toBeInTheDocument();
  });
  test("Shows cover", () => {
    expect(screen.getByAltText("").closest("img").src).toEqual(
      "https://drb-files-qa.s3.amazonaws.com/covers/default/defaultCover.png"
    );
  });
  test("shows license", () => {
    expect(screen.getByText("Copyright: Unknown").closest("a").href).toContain(
      "/copyright"
    );
  });
});
