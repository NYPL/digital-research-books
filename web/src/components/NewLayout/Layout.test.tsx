import { render } from "../../__tests__/testUtils/render";
import { screen } from "@testing-library/react";
import React from "react";
import Layout from "./Layout";

describe("Layout component", () => {
  beforeEach(() => {
    render(
      <Layout>
        <div>Text in layout body</div>
      </Layout>
    );
  });

  test("should have text in layout body", () => {
    const text = screen.getByText("Text in layout body");
    expect(text).toBeInTheDocument();
  });
});
